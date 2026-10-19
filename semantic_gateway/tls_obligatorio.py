"""TLS obligatorio delante de riffq.

riffq ofrece TLS pero no lo exige: un cliente que no pide cifrado conversa en texto plano. Además, un balanceador con
listener TLS (NLB, ALB) no sirve, porque los clientes Postgres piden TLS dentro del propio protocolo: primero envían un
`SSLRequest` sin cifrar y solo después de la respuesta `S` empieza el handshake TLS.

`PuertaTLS` atiende el puerto público y habla ese comienzo del protocolo:

* `SSLRequest` → responde `S`, hace el handshake TLS y reenvía el tráfico descifrado a riffq, que escucha sin TLS en
  un puerto interno de 127.0.0.1;
* `GSSENCRequest` (cifrado GSSAPI, que no se ofrece) → responde `N` y espera el siguiente mensaje;
* `CancelRequest` (cancelar una consulta en curso; los drivers lo envían por una conexión nueva) → lo reenvía;
* cualquier otro inicio (cliente sin SSL) → error `28000` "SSL obligatorio" y cierra la conexión.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import struct
import threading

logger = logging.getLogger("semantic_gateway.tls")

_SSL_REQUEST = 80877103
_GSSENC_REQUEST = 80877104
_CANCEL_REQUEST = 80877102
_MAX_INICIO = 10_000  # largo máximo razonable de un mensaje de inicio
_ESPERA_INICIO = 10.0  # segundos para enviar el primer mensaje y completar el handshake TLS (conexiones colgadas)


def puerto_libre(host: str = "127.0.0.1") -> int:
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def _error_fatal(codigo: str, mensaje: str) -> bytes:
    """ErrorResponse del protocolo Postgres."""
    campos = b"".join(t + v.encode() + b"\0" for t, v in
                      ((b"S", "FATAL"), (b"V", "FATAL"), (b"C", codigo), (b"M", mensaje)))
    cuerpo = campos + b"\0"
    return b"E" + struct.pack("!i", len(cuerpo) + 4) + cuerpo


class PuertaTLS:
    """Acepta conexiones en `host:port`, exige TLS y las reenvía a riffq en `127.0.0.1:destino`."""

    def __init__(self, host: str, port: int, destino: int, cert: str, key: str) -> None:
        self.host, self.port, self.destino = host, port, destino
        self.contexto = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.contexto.load_cert_chain(cert, key)
        self.listo = threading.Event()

    def start_in_thread(self) -> None:
        threading.Thread(target=lambda: asyncio.run(self._servir()), name="puerta-tls", daemon=True).start()
        if not self.listo.wait(10):
            raise RuntimeError(f"La puerta TLS no abrió el puerto {self.port}")

    async def _servir(self) -> None:
        servidor = await asyncio.start_server(self._atender, self.host, self.port)
        self.listo.set()
        async with servidor:
            await servidor.serve_forever()

    async def _atender(self, lector: asyncio.StreamReader, escritor: asyncio.StreamWriter) -> None:
        cliente = escritor.get_extra_info("peername")
        try:
            while True:
                cabecera = await asyncio.wait_for(lector.readexactly(8), _ESPERA_INICIO)
                largo, codigo = struct.unpack("!ii", cabecera)
                if not 8 <= largo <= _MAX_INICIO:
                    return
                if codigo == _GSSENC_REQUEST:
                    escritor.write(b"N")
                    await escritor.drain()
                    continue
                if codigo == _SSL_REQUEST:
                    escritor.write(b"S")
                    await escritor.drain()
                    await escritor.start_tls(self.contexto, ssl_handshake_timeout=_ESPERA_INICIO)
                    await self._reenviar(lector, escritor, b"")
                    return
                resto = await asyncio.wait_for(lector.readexactly(largo - 8), _ESPERA_INICIO)
                if codigo == _CANCEL_REQUEST:
                    await self._reenviar(lector, escritor, cabecera + resto)
                    return
                logger.warning("Conexión sin SSL rechazada desde %s", cliente[0] if cliente else "?")
                escritor.write(_error_fatal("28000", "El gateway exige SSL: conéctese con sslmode=require"))
                await escritor.drain()
                return
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ssl.SSLError) as e:
            if isinstance(e, ssl.SSLError):
                logger.warning("Handshake TLS fallido desde %s: %s", cliente[0] if cliente else "?", e.reason)
        finally:
            escritor.close()

    async def _reenviar(self, lector: asyncio.StreamReader, escritor: asyncio.StreamWriter, inicial: bytes) -> None:
        """Copia bytes en ambos sentidos entre el cliente (ya descifrado) y riffq."""
        for intento in range(50):  # riffq puede estar arrancando todavía
            try:
                r_lector, r_escritor = await asyncio.open_connection("127.0.0.1", self.destino)
                break
            except OSError:
                await asyncio.sleep(0.1)
        else:
            raise ConnectionError("riffq no responde en el puerto interno")
        if inicial:
            r_escritor.write(inicial)

        async def copiar(origen: asyncio.StreamReader, destino: asyncio.StreamWriter) -> None:
            try:
                while datos := await origen.read(65536):
                    destino.write(datos)
                    await destino.drain()
            except (ConnectionError, ssl.SSLError):
                pass
            finally:
                destino.close()

        await asyncio.gather(copiar(lector, r_escritor), copiar(r_lector, escritor))
