"""
    DDSI: SEMINARIO 1
    ACCESO A BASES DE DATOS
    
    GRUPO: A1_ddsimola:D
    MIEMBROS:
    - Luis Miguel Guirado Bautista
    - Pablo Irigoyen Cortadi
    - Linqi Zhu
    - Miguel Angel Serrano Villena
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
    print('\n\t--- MENÚ PRINCIPAL ---')
    print('\tOpciones:')
    print('\t1. Reestablecer tablas.')
    print('\t2. Dar de alta un pedido.')
    print('\t3. Mostrar contenido de las tablas.')
    print('\t4. Salir del programa y cerrar conexión.')


def escoger_opcion() -> int:
    """
        Solicita un número por entrada estándar
    """
    try:
        opc: int = int(input("\nElija una opción: "))
    except ValueError:
        print('Debe introducir un número')
        opc = -1
    return opc


def crear_tablas(conexion: oracledb.Connection):
    """
        Crea las tablas `Stock`, `Pedido` y `DetallePedido` definidas en el
        fichero `crear_tablas.sql` en la base de datos.
    """
    print('\nCreando tablas...\n') 
    with conexion.cursor() as cursor:
        # TABLA STOCK
        try:
            query = """CREATE TABLE Stock
                    (
                        Cproducto NUMBER CONSTRAINT Cproducto_no_nulo NOT NULL
                            CONSTRAINT Cproducto_clave_primaria PRIMARY KEY,
                        Cantidad NUMBER CONSTRAINT Cantidad_no_nulo NOT NULL
                    )
                    """
            cursor.execute(query)
        except Exception as e:
            print(f'No se ha podido crear la tabla Stock.\n {e}')                               

        # TABLA PEDIDO
        try:
            query = """CREATE TABLE Pedido
                    (
                        Cpedido NUMBER CONSTRAINT Cpedido_no_nulo NOT NULL
                            CONSTRAINT Cpedido_clave_primaria PRIMARY KEY,
                        Ccliente NUMBER CONSTRAINT Ccliente_no_nulo NOT NULL,
                        FechaPedido DATE CONSTRAINT FechaPedido_no_nulo NOT NULL    
                    )
                    """
            cursor.execute(query)
        except Exception as e:
            print(f'No se ha podido crear la tabla Pedido.\n {e}')

        # TABLA DETALLEPEDIDO
        try:
            query = """CREATE TABLE DetallePedido
                    (
                        Cpedido CONSTRAINT Cpedido_clave_externa_Pedido
                            REFERENCES Pedido (Cpedido),
                        Cproducto CONSTRAINT Cproducto_clave_externa_Stock
                            REFERENCES Stock (Cproducto),
                        Cantidad_pedido NUMBER CONSTRAINT Cantidad_pedido_no_nulo NOT NULL,
                        CONSTRAINT clave_primaria PRIMARY KEY (Cpedido, Cproducto)
                    )
                    """
            cursor.execute(query)
        except Exception as e:
            print(f'No se ha podido crear la tabla DetallePedido.\n {e}')
        
        finally:
            print('Se han creado las tablas correctamente.')
        print('\n')


def borrar_tablas(conexion: oracledb.Connection):
    """
        Borra las tablas Stock, Pedido y DetallePedido
        de la base de datos
    """
    print('\nBorrando tablas...\n') 
    def borrar_tabla(tabla: str):
        """
            Borra la tabla con nombre `tabla` de la base de datos
        """
        with conexion.cursor() as cursor:
            try:
                existe: bool = cursor.execute(
                    f'SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME=\'{tabla.upper()}\'').fetchone() != None
                if existe:
                    cursor.execute(f'DROP TABLE {tabla}')
                    print(f'Tabla {tabla} borrada correctamente.')
                else:
                    print(f'La tabla {tabla} no existe, no se puede borrar.')
                    return
            except Exception as e:
                print(f'No se ha podido borrar la tabla {tabla}. \n {e}')

    borrar_tabla('DetallePedido')
    borrar_tabla('Stock')
    borrar_tabla('Pedido')
    print('\n')


def insertar_tuplas_tabla_stock(conexion: oracledb.Connection):
    """
        Inserta 10 tuplas predefinidas en el fichero `insercion_tuplasPredefinidas_Stock.sql`
        en la tabla `Stock`
    """
    print('\nInsertando tuplas en la tabla Stock...\n') 
    with conexion.cursor() as cursor:
        try:
            cantidades: list[int] = [100, 50, 1000, 85, 21, 78, 101, 64, 37, 29]
            tuplas: list[tuple] = list(enumerate(cantidades, start=1))
            for cproducto, cantidad in tuplas:
                cursor.execute(
                    f'INSERT INTO Stock VALUES ({cproducto},{cantidad})')
        except Exception as e:
            print(f'No se pueden insertar las tuplas en Stock:\n {e}')
        else:
            print('Se han insertado las tuplas correctamente.')
    print('\n')


def mostrar_bd(conexion: oracledb.Connection):
    """
        Muestra las tablas Stock, Pedido y DetallePedido
        en la pantalla
    """
    def mostrar_tabla(tabla: str):
        """
            Muestra la tabla con nombre `tabla`
        """
        print(f'\n\t---Tabla {tabla}---\n\t')
        with conexion.cursor() as cursor:
            try:
                # Primero obtenemos los nombres de las columnas de la tabla
                query: str = f'SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME=\'{tabla.upper()}\''
                columnas: list = cursor.execute(query).fetchall()

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
                    print('No existen tuplas en esta tabla.')
            except Exception as e:
                print(f'Hubo un error al intentar mostrar la tabla {tabla}:\n {e}')

    mostrar_tabla('Stock')
    mostrar_tabla('Pedido')
    mostrar_tabla('DetallePedido')
    print('\n')


def alta_pedido(conexion: oracledb.Connection):

    # FUNCIONES ENCAPSULADAS ------------------------------------------------------------------------------------------------

    def crear_pedido():
        """
            Crea el pedido solicitando al usuario los tres campos necesarios.
            - El codigo del pedido
            - El codigo del cliente
            - La fecha del pedido
            Devuelve el código del pedido.
        """
        codigo_pedido  = int(input('\nIntroduzca el código del pedido: '))
        codigo_cliente = int(input('Introduzca el código del cliente: '))
        fecha_pedido   = input('Introduzca la fecha del pedido (DD/MM/AAAA): ')

        print('\nCreando el pedido...')
        try:
            query = f"INSERT INTO Pedido VALUES ({codigo_pedido}, {codigo_cliente}, TO_DATE('{fecha_pedido}', 'dd/mm/yyyy'))"
            conexion.cursor().execute(query)
        except Exception as e:
            print(f'\nNo se ha podido procesar el pedido:\n {e}')
            codigo_pedido = -1
        else:
            print('\n\tEl pedido se ha procesado correctamente.')
        return codigo_pedido


    def menu_pedido():
        """
            Imprime el menú secundario correspondiente a la funcionalidad
            de dar de alta un pedido
        """
        print('\n\t--- MENÚ DE ALTA DE PEDIDO ---')
        print('\tOpciones:')
        print('\t1. Añadir los detalles del pedido.')
        print('\t2. Eliminar todos los detalles del pedido.')
        print('\t3. Cancelar.')
        print('\t4. Terminar.')


    def aniadir_detalle():
        """
            Le pide al usuario que inserte detalles sobre el pedido que va a realizar:
            - El código del producto
                - Debe existir en la base de datos y debe haber existencias de ese producto
            - La cantidad que desea
                - Debe ser un número del intervalo `(0, cantidad_bd]`
            Una vez terminado, se inserta en la base de datos una nueva tupla en `DetallePedido`
            con los datos solicitados
        """
        codigo_producto: str = None
        cantidad_bd: int = 0
        cantidad_cliente: int = 0

        with conexion.cursor() as cursor:
            # Le preguntamos al usuario cual es el codigo de producto que desea
            # y obtenemos la cantidad del producto correspondiente de la tabla Stock
            while (cantidad_bd <= 0):
                try:
                    codigo_producto = int(input("\nInserte código de producto: "))
                    query = f'SELECT Cantidad FROM Stock WHERE Cproducto={codigo_producto}'
                    # Primera tupla de la consulta, primer valor
                    cantidad_bd = cursor.execute(query).fetchone()[0]
                    # Si el codigo de producto no existe, volvemos al menu de pedido para que vuelva a introducir los datos
                    if (cantidad_bd == None):
                        print('\tProducto no encontrado, volviendo al menu...') 
                        return
                # Si le pasamos una entrada (codigo_producto) con caracteres alfabéticos, volvemos atrás
                except ValueError:
                    print('Entrada no válida, cancelando...')
                    return
                # Si le pasamos un código de producto que no existe en la tabla Stock, volvemos atrás
                except TypeError:
                    print('El producto no existe, cancelando...')
                    return
                # Si ocurre otro error, volvemos atrás
                except Exception as e:
                    print(f'Ha ocurrido un error inseperado:\n {e}')
                    return
                else:
                    # Si no hay stock disponible, volvemos al menú anterior
                    if (cantidad_bd <= 0):
                        print('\n\tNo hay existencias del producto solicitado. Volviendo al menú...')
                        return
                    # Si hay stock, comunicamos cuantas unidades hay y seguimos
                    else:
                        print(f'\n\tExisten {cantidad_bd} unidades del producto solicitado.')

            # Ahora le pedimos al usuario la cantidad de productos que desea pedir
            # Debe ser menor o igual que la cantidad que hay en stock, y mayor o igual que cero
            while (cantidad_cliente <= 0 or cantidad_cliente > cantidad_bd):
                cantidad_cliente = int(input('\nCantidad a pedir (<=0 para cancelar): '))
                # Si le pasamos un valor menor o igual que cero, cancelamos:
                if (cantidad_cliente <= 0):
                    print('\n\tOperación cancelada, volviendo al menú...')
                    return
                # Si pedimos mas de lo que hay en Stock:
                if (cantidad_cliente > cantidad_bd):
                    print('\n\tLa cantidad debe ser menor o igual a las existencias disponibles.')
            
            # Finalmente, insertamos los detalles del pedido en la base de datos, si los detalles se han introducido
            # correctamente, actualizamos la columna cantidad del producto correspondiente
            try:
                query = f'INSERT INTO DetallePedido VALUES ({codigo_pedido}, {codigo_producto}, {cantidad_cliente})'
                cursor.execute(query)
            except Exception as e:
                print (f'\nNo se ha podido insertar el detalle del pedido:\n {e}')
            else:
                # TRAS HABERSE PROCESADO CORRECTAMENTE EL DETALLE DEL PEDIDO, SE ACTUALIZAN LAS EXISTENCIAS EN LA TABLA STOCK
                print('\nSe ha detallado el pedido correctamente.')
                query = f'UPDATE Stock SET Cantidad = Cantidad-{cantidad_cliente} WHERE Cproducto = {codigo_producto}'
                cursor.execute(query)


    def eliminar_detalle():
        """
            Eliminar todos los detalles del pedido
        """
        print('\nBorrando detalles del pedido...\n')
        with conexion.cursor() as cursor:
            try:
                cursor.execute('ROLLBACK TO detalle_pedido')
            except:
                print('\n\tError al intentar borrar los detalles del pedido.')
            else:
                print('\n\tDetalles del pedido borrados correctamente.')


    def cancelar():
        """
            Borrar tupla del pedido y salir
        """
        print('\nCancelando pedido...\n')
        with conexion.cursor() as cursor:
            try:
                cursor.execute('ROLLBACK TO pedido')
            except:
                print('Error al cancelar el pedido')
            else:
                print('Pedido cancelado correctamente')

    def terminar():
        # COMPROBAR SI AL FINAL EL PEDIDO TIENE ALGÚN DETALLE ASOCIADO
        print('\nConfirmando pedido...\n')
        query = f'SELECT count(*) FROM DetallePedido where Cpedido = {codigo_pedido}'
        with conexion.cursor() as cursor:
            num_tuplas_DetallePedido = cursor.execute(query).fetchone()[0]
            # Si el pedido tiene algún detalle asociado: correcto, confirmamos los pedidos
            if (num_tuplas_DetallePedido > 0):
                cursor.execute('COMMIT')
            else: # Si está vacío de detalles, no se guardará
                cursor.execute('ROLLBACK')

    # ----------------------------------------------------------------------------------------------------------------------

    codigo_pedido: int = -1
    with conexion.cursor() as cursor:
        # SE CREA EL PEDIDO ANTES DE PASAR A LOS DETALLES DEL PEDIDO
        cursor.execute('SAVEPOINT pedido') # Punto de guardado 1: justo antes de crear el pedido (rollback en cancelar())
        codigo_pedido = crear_pedido()

        if (codigo_pedido != -1):

            cursor.execute('SAVEPOINT detalle_pedido')  # Punto de guardado 2: justo antes de crear los detalles del pedido (rollback en eliminar_detalle())

            opc: int = None
            # Mientras no se cancele o se termine el pedido
            while (opc not in [3, 4]):
                menu_pedido()
                opc = escoger_opcion()
                match opc:
                    case 1:
                        aniadir_detalle()
                    case 2:
                        eliminar_detalle() # Deshacer los detalles del pedido, volvemos al punto 2
                    case 3:
                        cancelar() # Deshacemos el pedido, volvemos al punto 1
                    case 4:
                        terminar()
                    case _:
                        print('\nEsta opción no existe\n')
                # SIEMPRE muestra la base de datos si no se termina un pedido
                if (opc != 4):
                    mostrar_bd(conexion)

    print('\n\tSaliendo del menú de alta de pedido...')


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
        print('\n\tConectando a la base de datos...')
        conexion: oracledb.Connection = oracledb.connect(host='oracle0.ugr.es',
                                                         port='1521',
                                                         service_name='practbd.oracle0.ugr.es',
                                                         user=username,
                                                         password=password)
    except Exception as e:
        print(f'\n\tNo se ha podido establecer conexión con la base de datos: \n {e}')
        exit()
    else:
        print('\n\tConexión realizada correctamente.')

    # * ---------- Ahora podemos operar en la base de datos ----------

    OPCION_SALIR: int = 4
    opc: int = None

    while (opc != OPCION_SALIR):
        menu_principal()
        opc = escoger_opcion()
        match opc:
            case 1:
                # Borrado y nueva creación de las tablas e inserción de 10 tuplas predefinidas en el código en la tabla Stock
                borrar_tablas(conexion)
                crear_tablas(conexion)
                insertar_tuplas_tabla_stock(conexion)
                conexion.cursor().execute("COMMIT") # FALTABA UN COMMIT AL FINAL DE LA OPCIÓN 1 PARA QUE SE MANTUVIERAN ALGUNOS DE LOS CAMBIOS.
                print('\n\tTablas reestablecidas')
            case 2:
                # Dar de alta un pedido
                alta_pedido(conexion)
            case 3:
                # Mostrar el contenido de las tablas
                mostrar_bd(conexion)
            case _: # default:
                print('\nEsta opción no existe\n')

    # Cerramos conexión con la base de datos
    print("\nCerrando conexión...")
    conexion.close()

###############################################################################


if __name__ == "__main__":
    main()
