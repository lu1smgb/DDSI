# Seminario 1
## Acceso a bases de datos

### Uso del programa:

- Introducir los datos a través de la entrada estándar

    `$ py main.py`

- Pasar el usuario como parámetro y la contraseña por la entrada estándar

    `$ py main.py <usuario>`

- Pasar un fichero `TOML` con el usuario y la contraseña

    ```toml
    # Estructura del fichero TOML
    username = '...'
    password = '...'
    ```

    `$ py main.py <fichero>.toml`

- Introducir el usuario y la contraseña por parámetros
  
    `$ py main.py <usuario> <contraseña>`
    
