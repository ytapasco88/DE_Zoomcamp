# DE_Zoomcamp

Apuntes del curso

* Para configurar SSH en WSL:

```bash
sudo rm -rf .ssh -> para borrar carpeta oculta


ls -al
ssh -T git@github.com
cd .ssh
ls -al
ssh-keyscan github.com >> ~/.ssh/known_hosts

ssh-keygen -o -t rsa -C “ssh@github.com”

```

* Damos enter en las opciones que salen y en la contraseña colocamos la que vamos a usar para la conexion
* Luego de lo anterior, hacemos lo siguiente:

```bash
ls
cat id_rsa.pub

```

* Luego de copiado la info del cat, pegamos la public key en el administrador de keys de github, en la seccion de ssh
* Una vez hecho lo anterior, hacemos

```bash
git clone  git@github.com:<RUTA DEL REPO EN GIT DESDE SSH>.git
```

ahora estoy desde la rama linux