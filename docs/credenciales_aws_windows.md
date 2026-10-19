# Credenciales de AWS en Windows para el gateway semántico

Cómo configurar en Windows (PowerShell) las credenciales temporales de AWS para que el gateway ejecute en Athena.
No hace falta la AWS CLI: el gateway usa boto3, que se instala con `requirements-gateway.txt`.

> Las credenciales nunca van dentro del repo. En este documento se guardan en una carpeta **fuera** de
> `dbt-semantic-layer`. No las pegues en chats, tickets ni commits.

## Índice

1. [Qué se necesita](#1-qué-se-necesita)
2. [Preparar el entorno de Python](#2-preparar-el-entorno-de-python)
3. [Opción A: variables de entorno (una terminal)](#3-opción-a-variables-de-entorno-una-terminal)
4. [Opción B: perfil en la carpeta por defecto](#4-opción-b-perfil-en-la-carpeta-por-defecto)
5. [Opción C (recomendada): perfil en una carpeta propia](#5-opción-c-recomendada-perfil-en-una-carpeta-propia)
6. [Elegir el workgroup de Athena](#6-elegir-el-workgroup-de-athena)
7. [Levantar el gateway](#7-levantar-el-gateway)
8. [Renovar las credenciales](#8-renovar-las-credenciales)
9. [Problemas frecuentes](#9-problemas-frecuentes)
10. [Alternativa sin copiar credenciales: AWS SSO](#10-alternativa-sin-copiar-credenciales-aws-sso)

---

## 1. Qué se necesita

| Dato | De dónde sale |
| --- | --- |
| ID de clave de acceso (`ASIA…`) | Portal de acceso de AWS (SSO) → cuenta → *Access keys* |
| Clave de acceso secreta | Mismo lugar |
| Token de sesión | Mismo lugar. Las tres son **temporales**: duran entre 1 y 12 horas |
| Región | Donde están Glue y Athena de la cuenta (`us-east-1`) |
| Workgroup de Athena | Ver [sección 6](#6-elegir-el-workgroup-de-athena) |

Estructura de carpetas usada en este documento:

```
C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica\
├── .aws\
│   ├── credentials          ← claves (opción C)
│   └── config               ← región (opción C)
├── aws_cenco.ps1            ← script que activa el perfil en la terminal
├── dbt_venv\                ← entorno virtual de Python
└── dbt-semantic-layer\      ← repo (sin credenciales)
```

---

## 2. Preparar el entorno de Python

En PowerShell no existe `source`. El entorno virtual se activa con `Activate.ps1`:

```powershell
cd "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica"

# crear (una vez) con Python 3.12: riffq, el servidor del gateway, trae paquete precompilado para esa versión
py -3.12 -m venv dbt_venv
.\dbt_venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r dbt-semantic-layer\requirements-gateway.txt   # incluye boto3 y pyathena

# activar (cada terminal nueva)
.\dbt_venv\Scripts\Activate.ps1
```

Si sale *«la ejecución de scripts está deshabilitada en este sistema»*:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

---

## 3. Opción A: variables de entorno (una terminal)

La más rápida. Valen solo en la ventana de PowerShell donde se escriben:

```powershell
$env:AWS_ACCESS_KEY_ID     = "ASIA..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN     = "..."
$env:AWS_DEFAULT_REGION    = "us-east-1"
```

Comprobar:

```powershell
python -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
```

Debe mostrar `arn:aws:sts::<ID_CUENTA>:assumed-role/AWSReservedSSO_<ROL>/<tu correo>`.

---

## 4. Opción B: perfil en la carpeta por defecto

boto3 busca perfiles en `C:\Users\<usuario>\.aws\`. Sirve para varias terminales, pero los archivos quedan en el perfil
de Windows.

```powershell
New-Item -ItemType Directory -Force "$HOME\.aws" | Out-Null
@"
[cenco]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = ...
"@ | Set-Content -Encoding ascii "$HOME\.aws\credentials"
"[profile cenco]`nregion = us-east-1" | Set-Content -Encoding ascii "$HOME\.aws\config"

$env:AWS_PROFILE = "cenco"
python -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
```

Si ya existían esos archivos con otros perfiles, agregar las secciones al final en vez de sobrescribirlos.

---

## 5. Opción C (recomendada): perfil en una carpeta propia

Las credenciales quedan en `CapaSemantica\.aws\`, fuera del repo. boto3 las encuentra gracias a dos variables:
`AWS_SHARED_CREDENTIALS_FILE` y `AWS_CONFIG_FILE`.

### 5.1 Crear los archivos

Crearlos desde PowerShell, **no con el Bloc de notas**: el Bloc de notas les agrega `.txt` al nombre y puede cambiar la
codificación, y boto3 deja de encontrarlos.

```powershell
$base = "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica\.aws"
New-Item -ItemType Directory -Force $base | Out-Null

@"
[cenco]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = ...
"@ | Set-Content -Encoding ascii "$base\credentials"

"[profile cenco]`nregion = us-east-1" | Set-Content -Encoding ascii "$base\config"

Get-ChildItem $base     # deben aparecer "credentials" y "config", SIN .txt
```

Reglas de formato:

| Archivo | Encabezado | Contenido |
| --- | --- | --- |
| `credentials` | `[cenco]` | Las tres claves, sin comillas |
| `config` | `[profile cenco]` | `region = us-east-1` |

### 5.2 Script para activar el perfil

Crear `CapaSemantica\aws_cenco.ps1` con este contenido:

```powershell
$base = "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica\.aws"
$env:AWS_SHARED_CREDENTIALS_FILE = "$base\credentials"
$env:AWS_CONFIG_FILE             = "$base\config"
$env:AWS_PROFILE                 = "cenco"
# que no queden credenciales sueltas de la opción A (tienen prioridad sobre el perfil)
Remove-Item Env:AWS_ACCESS_KEY_ID, Env:AWS_SECRET_ACCESS_KEY, Env:AWS_SESSION_TOKEN -ErrorAction SilentlyContinue
python -c "import boto3; print('AWS:', boto3.client('sts').get_caller_identity()['Arn'])"
```

Si se crea con el Bloc de notas, comprobar que no quedó como `aws_cenco.ps1.txt`.

### 5.3 Cargarlo en cada terminal

El script se ejecuta con **punto y espacio** delante (`. `): así las variables quedan en la terminal. Sin el punto, se
pierden al terminar el script.

```powershell
# desde CapaSemantica
. .\aws_cenco.ps1

# desde dbt-semantic-layer
. ..\aws_cenco.ps1

# desde cualquier carpeta
. "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica\aws_cenco.ps1"
```

Debe imprimir `AWS: arn:aws:sts::<ID_CUENTA>:assumed-role/…`. El gateway tiene que levantarse **en esa misma
terminal**.

---

## 6. Elegir el workgroup de Athena

Listar los workgroups de la cuenta y su ruta de resultados:

```powershell
python -c "import boto3; a=boto3.client('athena'); [print(w['Name'], '|', a.get_work_group(WorkGroup=w['Name'])['WorkGroup']['Configuration'].get('ResultConfiguration',{}).get('OutputLocation','(sin ruta)'), '| obligatoria:', a.get_work_group(WorkGroup=w['Name'])['WorkGroup']['Configuration'].get('EnforceWorkGroupConfiguration')) for w in a.list_work_groups()['WorkGroups']]"
```

En la cuenta de IT People el resultado fue:

| Workgroup | Ruta de resultados | Obligatoria | Uso |
| --- | --- | --- | --- |
| `DataEngineer` | `s3://…-athena-query-results-…/data-engineer/` | Sí | **Usar este**: no hace falta `--athena-output` |
| `primary` | (sin ruta) | No | Requiere `--athena-output` y permisos de escritura en esa ruta |

Comprobar que el rol ve la tabla del manifest:

```powershell
python -c "import boto3; t=boto3.client('glue').get_table(DatabaseName='dlk_gld_it_people_dwh', Name='fact_empleado')['Table']; print(t['Name'], t['StorageDescriptor'].get('Location'))"
```

---

## 7. Levantar el gateway

En la terminal donde se cargó el perfil:

```powershell
cd "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica"
.\dbt_venv\Scripts\Activate.ps1
. .\aws_cenco.ps1
cd dbt-semantic-layer

python -m semantic_gateway --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer
```

Para cifrar la conexión agregar `--tls-cert <ruta>\server.crt --tls-key <ruta>\server.key --tls-obligatorio`
(los archivos fuera del repo; en DBeaver, pestaña *SSL* en modo `require`).

En PowerShell la línea se continúa con acento grave (`` ` ``), no con `\`. Con la opción C (o `AWS_PROFILE` definido) no
hace falta `--aws-profile`; con la opción B sin `AWS_PROFILE`, agregar `--aws-profile cenco`.

Al arrancar debe mostrar:

```
Ejecución en Athena · workgroup=DataEngineer · resultados=(la del workgroup) · región=us-east-1 · … · timeout=100 s
Gateway listo en 127.0.0.1:5433 · semantic.{employee_headcount} · … · ejecuta en: athena …
```

Conexión en DBeaver: PostgreSQL · `127.0.0.1` · puerto `5433` · base `athena` · usuario `bi` · clave `bi`.

Si un cliente falla con el servidor actual (riffq), se puede levantar el anterior (buenavista) con las mismas opciones:

```powershell
python -m semantic_gateway.server_old --manifest semantic_manifests/it-people/semantic_manifest.json --motor athena --ejecutar `
  --region us-east-1 --athena-workgroup DataEngineer
```

---

## 8. Renovar las credenciales

Cuando venzan, Athena responde `ExpiredToken` (o `Unable to locate credentials` si el archivo quedó vacío).

1. Obtener credenciales nuevas en el portal de AWS.
2. Reescribir `CapaSemantica\.aws\credentials` con el bloque de la [sección 5.1](#51-crear-los-archivos).
3. Reiniciar el gateway (`Ctrl+C` y volver a lanzarlo). No hace falta volver a cargar el script en esa terminal.

Con la opción A, volver a definir las tres variables y reiniciar el gateway.

---

## 9. Problemas frecuentes

| Mensaje | Causa | Solución |
| --- | --- | --- |
| `El término 'source' no se reconoce…` | `source` es de Mac/Linux | `.\dbt_venv\Scripts\Activate.ps1` |
| `El término 'aws' no se reconoce…` | La AWS CLI no está instalada | No hace falta; comprobar con `python -c "import boto3; …"`. Para instalarla: `winget install -e --id Amazon.AWSCLI` |
| `El término '..\aws_cenco.ps1' no se reconoce…` | Ruta relativa equivocada o archivo guardado como `.ps1.txt` | Desde `CapaSemantica` usar `. .\aws_cenco.ps1`; revisar el nombre con `Get-ChildItem` |
| La ejecución de scripts está deshabilitada | Política de ejecución de PowerShell | `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned` |
| `ProfileNotFound: The config profile (cenco) could not be found` | `config` no existe, se llama `config.txt` o el encabezado no es `[profile cenco]` | Renombrar o recrear con `Set-Content -Encoding ascii` (sección 5.1) |
| `NoCredentialsError: Unable to locate credentials` | `credentials` se llama `credentials.txt`, el encabezado no es `[cenco]`, faltan claves, o el gateway corre en otra terminal sin el script cargado | Revisar el archivo; cargar `. .\aws_cenco.ps1` en la misma terminal del gateway |
| `ExpiredToken` | Vencieron las credenciales temporales | [Sección 8](#8-renovar-las-credenciales) |
| `AccessDenied` (Athena, S3 o Lake Formation) | El rol no tiene permiso sobre la tabla, sus datos o la ruta de resultados | Pedir el permiso o usar el workgroup del rol (`DataEngineer`) |

Comprobaciones rápidas del archivo de credenciales (sin mostrar las claves):

```powershell
$base = "C:\Users\<usuario>\Documents\Cencosud\DATA CORE\CapaSemantica\.aws"
Get-ChildItem $base                                             # "credentials" y "config", sin .txt
Get-Content "$base\credentials" | Select-String '^\['           # debe mostrar [cenco]
(Get-Content "$base\credentials" | Select-String '=').Count     # debe ser 3
echo $env:AWS_PROFILE $env:AWS_SHARED_CREDENTIALS_FILE           # cenco y la ruta del archivo
```

---

## 10. Alternativa sin copiar credenciales: AWS SSO

El rol de la cuenta es de SSO (`AWSReservedSSO_…`). Con la AWS CLI instalada se puede configurar el perfil una sola vez
y renovarlo desde el navegador, sin copiar claves:

```powershell
winget install -e --id Amazon.AWSCLI       # cerrar y abrir PowerShell después
aws configure sso --profile cenco          # pide la URL del portal SSO, la cuenta y el rol
aws sso login --profile cenco              # cada vez que venza la sesión
```

El gateway usa ese perfil igual (`$env:AWS_PROFILE = "cenco"` o `--aws-profile cenco`).
