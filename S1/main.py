"""
    DDSI: SEMINARIO 1
    ACCESO A BASES DE DATOS
    
    GRUPO: A1_ddsimola:D
    MIEMBROS:
    - Luis Miguel Guirado Bautista (lu1smgb)
    - Pablo Irigoyen Cortadi (PIrigoyen)
    - Linqi Zhu (zlq07)
    - Miguel Angel Serrano Villena (migue-maca-IngInfo)
"""

# encoding: utf-8
###############################################################################
import oracledb
from getpass import getpass
from sys import argv
import toml
import pandas as pd
###############################################################################


def menu_principal():
    """
        Imprime el menú principal por pantalla
    """
    print('Opciones:')
    print('\t1. Borrado y nueva creación de las tablas e inserción de 10 tuplas predefinidas en el código en la tabla Stock')
    print('\t2. Dar de alta un pedido')
    print('\t3. Mostrar contenido de las tablas')
    print('\t4. Salir del programa y cerrar conexión')


def escoger_opcion() -> int:
    """
        Solicita un número por entrada estándar
    """
    return int(input("Elija una opción: "))


def crear_tablas(conexion: oracledb.Connection):
    """
        Crea las tablas `Stock`, `Pedido` y `DetallePedido` definidas en el
        fichero `crear_tablas.sql` en la base de datos.
    """
    with conexion.cursor() as cursor:
        try:
            query = """
CREATE TABLE Stock
(
    Cproducto NUMBER CONSTRAINT Cproducto_no_nulo NOT NULL
        CONSTRAINT Cproducto_clave_primaria PRIMARY KEY,
    Cantidad NUMBER CONSTRAINT Cantidad_no_nulo NOT NULL
)
"""
            cursor.execute(query)
        except Exception as e:
            print(f'No se pueden crear las tablas \n {e}')
        else:
            print('Se han creado las tablas correctamente')


def borrar_tablas(conexion: oracledb.Connection):
    """
        Borra las tablas Stock, Pedido y DetallePedido
        de la base de datos
    """
    def borrar_tabla(tabla: str):
        """
            Borra la tabla con nombre `tabla` de la base de datos
        """
        with conexion.cursor() as cursor:
            try:
                # TODO: Borrar tabla
                # ? Verificamos primero que la tabla existe antes de borrarla?
                # ? Si no, saltara un Exception
                existe: bool = cursor.execute(
                    f'SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME=\'{tabla.upper()}\'').fetchone() != None
                if existe:
                    cursor.execute(f'DROP TABLE {tabla}')
                    print(f'Tabla {tabla} borrada correctamente')
                else:
                    print(f'La tabla {tabla} no existe, no se puede borrar')
                    return
            except Exception as e:
                print(f'No se ha podido borrar la tabla {tabla} \n {e}')

    borrar_tabla('DetallePedido')
    borrar_tabla('Stock')
    borrar_tabla('Pedido')


def insertar_tuplas_tabla_stock(conexion: oracledb.Connection):
    """
        Inserta 10 tuplas predefinidas en el fichero `insercion_tuplasPredefinidas_Stock.sql`
        en la tabla `Stock`
    """
    with conexion.cursor() as cursor:
        try:
            cantidades: list[int] = [
                100, 50, 1000, 85, 21, 78, 101, 64, 37, 29]
            tuplas: list[tuple] = list(enumerate(cantidades, start=1))
            for cproducto, cantidad in tuplas:
                cursor.execute(
                    f'INSERT INTO Stock VALUES ({cproducto},{cantidad})')
        except Exception as e:
            print(f'No se pueden insertar las tuplas en Stock: \n {e}')
        else:
            print('Se han insertado las tuplas correctamente')


def mostrar_bd(conexion: oracledb.Connection):
    """
        Muestra las tablas Stock, Pedido y DetallePedido
        en la pantalla
    """
    def mostrar_tabla(tabla: str):
        """
            Muestra la tabla con nombre `tabla`
        """
        print(f'\t---Tabla {tabla}---\t')
        with conexion.cursor() as cursor:
            try:
                # Primero obtenemos los nombres de las columnas de la tabla
                columnas: list = cursor.execute(
                    f'SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME=\'{tabla.upper()}\'').fetchall()

                # Apareceran tuplas de tamaño 1, nos quedamos solo con su contenido
                columnas = [t[0] for t in columnas]

                # Despues construimos una tabla de Pandas (dataframe) con las tuplas obtenidas
                # y los nombres de columnas que hemos obtenidos anteriormente
                dataframe: pd.DataFrame = pd.DataFrame(cursor.execute(
                    f'SELECT * FROM {tabla}').fetchall(), columns=columnas)

                # Mostramos la tabla
                if (not dataframe.empty):
                    print(dataframe.to_string(index=False))
                else:
                    print('No existen tuplas en esta tabla')

            except Exception as e:
                print(
                    f'Hubo un error al intentar mostrar la tabla {tabla}: \n {e}')

    mostrar_tabla('Stock')
    mostrar_tabla('Pedido')
    mostrar_tabla('DetallePedido')


def alta_pedido(conexion: oracledb.Connection):

    codigo_producto: str = None
    cantidad_bd: int = 0
    cantidad_cliente: int = 0

    def menu_pedido():
        """
            Imprime el menú secundario correspondiente a la funcionalidad
            de dar de alta un pedido
        """
        print('Opciones:')
        print('1. Añadir los detalles del pedido')
        print('2. Eliminar todos los detalles del pedido')
        print('3. Cancelar')
        print('4. Terminar')

    def aniadir_detalle():
        """
            Le pide al usuario que inserte detalles sobre el pedido que va a realizar:
            - El código del producto
                - Debe existir en la base de datos y debe haber existencias de ese producto
            - La cantidad que desea
                - Debe ser un número del intervalo `(0, cantidad_bd]`
            Una vez terminado, se inserta en la base de datos una nueva tupla en `DetallePedido`
            con los datos solicitados
            # TODO: Comentar acerca de la transferencia de datos
        """
        with conexion.cursor() as cursor:
            # Le preguntamos al usuario cual es el codigo de producto que desea
            # y obtenemos la cantidad del producto correspondiente de la tabla Stock
            while (cantidad_bd <= 0):
                try:
                    codigo_producto = str(
                        input("Inserte código de producto: "))
                    query = f'SELECT Cantidad FROM Stock WHERE Cproducto={codigo_producto}'
                    # Primera tupla de la consulta, primera columna
                    cantidad_bd = cursor.execute(query).fetchone()[0]
                except Exception as e:
                    print(f'Ha ocurrido un error en la base de datos: \n {e}')
                    return
                else:
                    # Si no hay stock disponible, volvemos al menú anterior
                    if (cantidad_bd <= 0):
                        print(
                            'No hay existencias del producto solicitado. Volviendo al menú...')
                        return
                    # Si hay stock, comunicamos cuantas unidades hay y seguimos
                    else:
                        print(
                            f'Existen {cantidad_bd} unidades del producto solicitado')

            # Ahora le pedimos al usuario la cantidad de productos que desea pedir
            # Debe ser menor o igual que la cantidad que hay en stock, y mayor o igual que cero
            while (cantidad_cliente <= 0 or cantidad_cliente > cantidad_bd):
                cantidad_cliente = int(input('Cantidad a pedir: '))
                if (cantidad_cliente > cantidad_bd):
                    print(
                        'La cantidad debe ser menor o igual a las existencias disponibles')

            # TODO: Resto de funcion añadir detalle
            # Una vez tenemos bien todos los datos solicitados, creamos una nueva tupla en
            # la tabla DetallesPedido (Cpedido, Cproducto, cantidad)
            #! Tambien deberiamos hacer lo de las transferencias

            # Finalmente:
            # print('Se ha detallado el pedido correctamente')
            # print('Codigo pedido')
            # print('Codigo producto')
            # print('Cantidad')
            # ------------------------------------------------

    # TODO: Eliminar detalle
    def eliminar_detalle():
        """
            Eliminar todos los detalles SOLO en detalle-pedido
            #? Las variables del codigo de producto y de la cantidad deberian estar definidas en alta pedido?
        """
        with conexion.cursor() as cursor:
            try:
                pass
            except:
                print('Error al intentar borrar los detalles del pedido')
            else:
                print('Detalles del pedido borrados correctamente')

    # TODO: Cancelar pedido
    def cancelar():
        """
            borrar tupla de pedido y salir
        """
        with conexion.cursor() as cursor:
            try:
                pass
            except:
                print('Error al cancelar el pedido')
            else:
                print('Pedido cancelado correctamente')

    def terminar():
        print('Saliendo del menú de altas...')

    # Crear pedido
    menu_pedido()
    opc = None
    # Mientras no se cancele o se termine el pedido
    while (opc not in [3, 4]):
        opc = escoger_opcion()
        match opc:
            case 1:
                aniadir_detalle()
            case 2:
                eliminar_detalle()
            case 3:
                cancelar()
            case 4:
                terminar()
        if (opc != 4):
            mostrar_bd(conexion)

###############################################################################


def main():
    """
    Uso del programa:

        - `py main.py`                          -> Datos por entrada estándar
        - `py main.py <nombre_archivo>.toml`    -> Datos por fichero TOML
        - `py main.py <usuario>`                -> Usuario por parámetros y contraseña por entrada estándar
        - `py main.py <usuario> <contraseña>`   -> Datos por argumentos
    """

    # * ---------- Obtenemos el usuario y la contraseña ----------

    username: str = None
    password: str = None

    # Si no le pasamos argumentos nos pide los datos de login por la entrada estándar
    if (len(argv) == 1):
        username = getpass('Usuario: ')

    # Si le pasamos uno:
    elif (len(argv) == 2):

        # Si es un fichero .toml, importa los datos de login desde ese archivo
        """
            Formato del archivo TOML:
            username = 'x0000000'
            password = 'x0000000'
        """
        if (argv[1].endswith('.toml')):
            try:
                params = toml.load(argv[1])
            except Exception as e:
                print(
                    f'Se pasó un fichero TOML, pero no pudo cargarse correctamente: \n {e}')
                exit()
            else:
                username, password = params['username'], params['password']

        # Si no, decide que es el nombre de usuario
        else:
            username = argv[1]

    # Si le pasamos 2, suponemos que son el usuario y la contraseña
    elif (len(argv) == 3):
        username, password = argv[1], argv[2]

    # Si no le pasamos una contraseña
    if (password == None):
        password = getpass('Contraseña: ')

    # * ---------- Establecemos la conexión ----------

    try:
        print('Conectando a la base de datos...')
        conexion: oracledb.Connection = oracledb.connect(host='oracle0.ugr.es',
                                                         port='1521',
                                                         service_name='practbd.oracle0.ugr.es',
                                                         user=username,
                                                         password=password)
    except Exception as e:
        print(
            f'No se ha podido establecer conexión con la base de datos: \n {e}')
        exit()
    else:
        print('Conexión realizada correctamente')

    # * ---------- Ahora podemos operar en la base de datos ----------

    menu_principal()
    opc: int = None
    while (opc != 4):
        opc = escoger_opcion()
        match opc:
            case 1:
                # TODO: Revisar. Deberiamos encapsular estas tres funciones??? -> reestablecer_bd()
                # Borrado y nueva creación de las tablas e inserción de 10 tuplas predefinidas en el código en la tabla Stock
                borrar_tablas(conexion)
                crear_tablas(conexion)
                insertar_tuplas_tabla_stock(conexion)
            case 2:
                # Dar de alta un pedido
                alta_pedido(conexion)
            case 3:
                # Mostrar el contenido de las tablas
                mostrar_bd(conexion)

    # Cerramos conexión con la base de datos
    print("Cerrando conexión...")
    conexion.close()

###############################################################################


if __name__ == "__main__":
    main()
