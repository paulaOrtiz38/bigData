import happybase
import pandas as pd
from datetime import datetime
from collections import defaultdict
# Bloque principal de ejecución

try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
    # 2. Crear la tabla con las familias de columnas
    table_name = 'ventas'
    families = {
        'factura': dict(),  # información factura
        'producto': dict(), # detalles del producto
        'envio': dict(),    # detalles del envio
        'ventas': dict(),    # detalle de la venta
        'estado': dict()    # estado de la venta
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'ventas' creada exitosamente")

    # 3. Cargar datos del CSV
    sales_data = pd.read_csv('ventas.csv')

     # Iterar sobre el DataFrame usando el índice
    for index, row in sales_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'sal_{index}'.encode()
        # Organizar los datos en familias de columnas
        data = {
            b'factura:factura_no': str(row['InvoiceNo']).encode(),
            b'factura:fecha_factura': str(row['InvoiceDate']).encode(),
            b'factura:id_cliente': str(row['CustomerID']).encode(),
            b'factura:pais': str(row['Country']).encode(),
            b'factura:metodo_pago': str(row['PaymentMethod']).encode(),
            b'factura:descuento': str(row['Discount']).encode(),
            b'producto:codigo_stock': str(row['StockCode']).encode(),
            b'producto:descripcion': str(row['Description']).encode(),
            b'producto:cantidad': str(row['Quantity']).encode(),
            b'producto:precio_unitario': str(row['UnitPrice']).encode(),
            b'producto:categoria': str(row['Category']).encode(),
            b'envio:costo_envio': str(row['ShippingCost']).encode(),
            b'envio:proveedor_envio': str(row['ShipmentProvider']).encode(),
            b'envio:ubicacion_almacen': str(row['WarehouseLocation']).encode(),
            b'ventas:canal_ventas': str(row['SalesChannel']).encode(),
            b'estado:estado_retorno': str(row['ReturnStatus']).encode(),
            b'estado:orden_prioridad': str(row['OrderPriority']).encode()
        }
        table.put(row_key, data)

    print("Datos cargados exitosamente")

     # 4. Consultas y Análisis de Datos
    # Diccionario para almacenar el total de ventas por categoría
    ventas_por_categoria = defaultdict(float)

    # Escanear la tabla
    for key, data in table.scan(columns=[b'producto:categoria', b'producto:cantidad', b'producto:precio_unitario']):
        categoria = data.get(b'producto:categoria', b'').decode('utf-8')
        cantidad = int(data.get(b'producto:cantidad', b'0'))
        precio_unitario = float(data.get(b'producto:precio_unitario', b'0.0'))
        
        ventas_por_categoria[categoria] += cantidad * precio_unitario

    print("Total de ventas por categoria:")

    for categoria, total in ventas_por_categoria.items():
        print(f"Categoría: {categoria}, Ventas: {total}")

    # 2.
    clientes_por_pais = defaultdict(set)
    # Escanear y contar clientes únicos por país
    for key, data in table.scan(columns=[b'factura:id_cliente', b'factura:pais']):
        cliente_id = data.get(b'factura:id_cliente', b'').decode('utf-8')
        pais = data.get(b'factura:pais', b'').decode('utf-8')        
        
        if cliente_id:
            clientes_por_pais[pais].add(cliente_id)
    
    print("Numero de clientes unicos por pais:")
    for pais, clientes in clientes_por_pais.items():
        print(f"País: {pais}, Clientes unicos: {len(clientes)}")

    # 3
    devoluciones_por_proveedor = defaultdict(int)

    # Escanear devoluciones por proveedor
    for key, data in table.scan(columns=[b'envio:proveedor_envio', b'estado:estado_retorno']):
        proveedor_envio = data.get(b'envio:proveedor_envio', b'').decode('utf-8')
        estado_retorno = data.get(b'estado:estado_retorno', b'').decode('utf-8')

        if estado_retorno == 'Pendiente':  # Cambiar 'Pendiente' si el estado es diferente
            devoluciones_por_proveedor[proveedor_envio] += 1

    print("Devoluciones por proveedor de envio:")
    for proveedor, devoluciones in devoluciones_por_proveedor.items():
        print(f"Proveedor: {proveedor}, Devoluciones pendientes: {devoluciones}")

    ingresos_por_canal = defaultdict(float)

    # Escanear ingresos por canal de ventas
    for key, data in table.scan(columns=[b'ventas:canal_ventas', b'producto:cantidad', b'producto:precio_unitario']):
        canal = data.get(b'ventas:canal_ventas', b'').decode('utf-8')
        cantidad = int(data.get(b'producto:cantidad', b'0'))
        precio_unitario = float(data.get(b'producto:precio_unitario', b'0.0'))
        
        ingresos_por_canal[canal] += cantidad * precio_unitario

    print("Ingresos por canal de ventas:")
    for canal, ingresos in ingresos_por_canal.items():
        print(f"Canal: {canal}, Ingresos: {ingresos}")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()