# Desplegar LifeMap en una máquina virtual de AWS Academy

Objetivo: que el sistema corra en un servidor distinto al PC de desarrollo y responda en la URL pública fija
(`https://reshape-varied-overlap.ngrok-free.dev`), con Jenkins desplegando solo cuando llegue algo a `main`.
La VM no necesita IP fija ni puertos abiertos al público: ngrok sale desde la VM hacia internet y mantiene el mismo dominio.

## Antes de empezar

- El código ya tiene que estar en GitHub (`https://github.com/biltormenta/Big-data-final`), en la rama `main`.
- Si el repositorio es privado, la VM no podrá clonarlo sin credenciales: hágalo público hasta la calificación o use un token de solo lectura.
- **Importante:** el PC de escritorio y la VM no pueden usar el mismo dominio de ngrok a la vez. Antes de encender la VM, apague el túnel del PC: `docker compose stop ngrok`.

## Pasos

1. **Entrar al laboratorio.** En AWS Academy: curso *Learner Lab* → *Start Lab* → esperar el círculo verde → *AWS* (abre la consola).
2. **Crear la instancia** (EC2 → *Launch instance*):
   - Imagen: **Ubuntu Server 24.04 LTS**.
   - Tipo: **t3.large** (2 vCPU y 8 GB de RAM; menos de 8 GB no alcanza para Spark + Mongo + Dask).
   - Par de claves: el que ofrece el laboratorio (`vockey`).
   - Disco: **50 GB** (gp3).
   - Grupo de seguridad: solo SSH (22). Nada más: el tráfico público entra por ngrok.
3. **Abrir una terminal:** en la instancia → *Connect* → pestaña **EC2 Instance Connect** → *Connect* (consola en el navegador, no hace falta la llave).
4. **Instalar y arrancar** (se pega en esa terminal):
   ```bash
   sudo apt-get update && sudo apt-get install -y git
   git clone https://github.com/biltormenta/Big-data-final.git lifemap
   cd lifemap
   bash despliegue/vm_instalar.sh
   ```
   El script pide una vez la URL del repo, el dominio de ngrok y el authtoken (el token no se ve al escribirlo), instala Docker, levanta todo y deja la carga de datos corriendo en segundo plano.
5. **Seguir el avance:** `tail -f carga.log`. Termina con `CARGA_TERMINADA`; en una VM de 2 núcleos tarda alrededor de 1 a 1,5 horas.
6. **Comprobar:** `https://reshape-varied-overlap.ngrok-free.dev/health` debe mostrar Mongo, Spark, Dask y la API en `ok`, y el hash del commit.
7. **Jenkins:** corre en la VM (puerto 8080, sin exponer). Para verlo desde el PC, un túnel SSH o abrir el 8080 en el grupo de seguridad solo para su IP. Usuario `admin`; la contraseña la imprime el script. Se dispara por `pollSCM` cada 2 minutos (la VM no es alcanzable por webhook sin otro túnel).

## Lo que hay que vigilar en AWS Academy

- **El laboratorio tiene sesiones de 4 horas.** Al vencer, la consola se cierra y, según la configuración del curso, las instancias pueden quedar *detenidas*. Si eso pasa, vuelva a pulsar *Start Lab* y arranque la instancia (*Instance state → Start*). Como Docker arranca solo al encenderse la VM y todos los servicios tienen `restart: unless-stopped`, todo vuelve solo y el túnel de ngrok reconecta aunque cambie la IP. Los datos no se pierden al detener la instancia (sí al terminarla: no use *Terminate*).
- **Presupuesto:** el laboratorio trae un tope en dólares; una t3.large consume poco, pero revise el contador en el panel.
- **Verificación recomendada:** prueben desde ya cuánto tiempo aguanta encendida la instancia con el laboratorio cerrado. Si se detiene, la VM sola no cubre las 48 horas.
- **Plan B:** el PC de escritorio sigue sirviendo como respaldo con el mismo dominio (`docker compose --profile ngrok up -d ngrok`, solo si la VM está apagada).
