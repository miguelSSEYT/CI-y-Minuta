import streamlit as st
import pandas as pd
from io import BytesIO

# ==============================
# FUNCION PRINCIPAL DE PROCESO FIFO
# ==============================
def procesar_fifo(file):
    # === 1. Leer el archivo
    minuta = pd.read_excel(file, sheet_name="Minuta")
    ci = pd.read_excel(file, sheet_name="CI")

    # === 2. Normalizar nombres
    minuta['Descripcion'] = minuta['Descripcion'].astype(str).str.strip().str.lower()
    ci['DES NO CUSTOM'] = ci['DES NO CUSTOM'].astype(str).str.strip().str.lower()

    # === 3. Preparar columnas extra en CI
    ci['Commodity Code3'] = None
    ci['Net Price'] = None
    ci['Linea Tipo'] = None

    # === 4. Lógica FIFO ===
    minuta_saldos = minuta.copy()
    result_ci = []

    for _, row in ci.iterrows():
        producto = row['DES NO CUSTOM']
        qty_needed = row['Delivery Quantity']

        # Filtrar minuta para ese producto
        filtro_producto = minuta_saldos[minuta_saldos['Descripcion'] == producto]

        if filtro_producto.empty:
            # No existe en minuta → línea maquila
            nueva_linea = row.copy()
            nueva_linea['Linea Tipo'] = 'línea de maquila'
            result_ci.append(nueva_linea)
            continue

        # Recorrer filas FIFO
        for i_m, fila_minuta in filtro_producto.iterrows():
            saldo = minuta_saldos.at[i_m, 'Saldo pendiente']

            if saldo <= 0:
                continue

            if qty_needed <= saldo:
                # Se puede surtir todo
                minuta_saldos.at[i_m, 'Saldo pendiente'] -= qty_needed

                nueva_linea = row.copy()
                nueva_linea['Commodity Code'] = fila_minuta['Fraccion']
                nueva_linea['Commodity Code3'] = fila_minuta['Desc Fraccion']
                nueva_linea['Net Price'] = fila_minuta['Precio Unitario']
                nueva_linea['Linea Tipo'] = 'línea de sse'
                result_ci.append(nueva_linea)

                qty_needed = 0
                break
            else:
                # Fraccionar
                nueva_linea_sse = row.copy()
                nueva_linea_sse['Delivery Quantity'] = saldo
                nueva_linea_sse['Commodity Code'] = fila_minuta['Fraccion']
                nueva_linea_sse['Commodity Code3'] = fila_minuta['Desc Fraccion']
                nueva_linea_sse['Net Price'] = fila_minuta['Precio Unitario']
                nueva_linea_sse['Linea Tipo'] = 'línea de sse'
                result_ci.append(nueva_linea_sse)

                # Reducir saldo
                minuta_saldos.at[i_m, 'Saldo pendiente'] = 0
                qty_needed -= saldo

        # Si falta cantidad → maquila
        if qty_needed > 0:
            nueva_linea_maquila = row.copy()
            nueva_linea_maquila['Delivery Quantity'] = qty_needed
            nueva_linea_maquila['Linea Tipo'] = 'línea de maquila'
            result_ci.append(nueva_linea_maquila)

    ci_modificado = pd.DataFrame(result_ci)
    minuta_actualizada = minuta_saldos

    return ci_modificado, minuta_actualizada


# ==============================
# INTERFAZ STREAMLIT
# ==============================
st.set_page_config(page_title="FIFO Minuta-CI", layout="wide")
st.title("🔄 Análisis FIFO Minuta vs CI")

st.write("Sube un archivo con las hojas **Minuta** y **CI** para procesar el análisis FIFO y generar los dos archivos resultantes:")

# Subida del archivo
file = st.file_uploader("Cargar archivo Excel", type=["xlsx"])

if file:
    # Procesar
    ci_modificado, minuta_actualizada = procesar_fifo(file)

    st.success("¡Procesamiento completado!")

    # Mostrar vistas previas
    st.subheader("Vista previa - CI modificado")
    st.dataframe(ci_modificado.head(20))

    st.subheader("Vista previa - Minuta actualizada")
    st.dataframe(minuta_actualizada.head(20))

    # Función para exportar a Excel
    def export_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        return output.getvalue()

    # Botones de descarga
    st.download_button(
        label="📥 Descargar CI Modificado",
        data=export_excel(ci_modificado),
        file_name="CI_modificado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.download_button(
        label="📥 Descargar Minuta Actualizada",
        data=export_excel(minuta_actualizada),
        file_name="Minuta_actualizada.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
