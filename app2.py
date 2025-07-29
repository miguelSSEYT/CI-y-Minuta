import streamlit as st
import pandas as pd
from io import BytesIO

# ==============================
# FUNCIONES AUXILIARES
# ==============================

def normalizar_columnas(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def encontrar_columna(df, keyword):
    for col in df.columns:
        if keyword in col:
            return col
    raise KeyError(f"No se encontró columna que contenga: {keyword}")

# ==============================
# PROCESO FIFO
# ==============================
def procesar_fifo(file):
    # Leer archivo
    minuta = pd.read_excel(file, sheet_name="Minuta")
    ci = pd.read_excel(file, sheet_name="CI")

    # Normalizar columnas
    minuta = normalizar_columnas(minuta)
    ci = normalizar_columnas(ci)

    # Identificar columnas clave en Minuta
    col_desc_minuta = encontrar_columna(minuta, "descripcion")
    col_saldo_minuta = encontrar_columna(minuta, "saldo")
    col_fraccion = encontrar_columna(minuta, "fraccion")
    col_desc_fraccion = encontrar_columna(minuta, "desc fraccion")
    col_precio = encontrar_columna(minuta, "precio")
    col_delivery_minuta = encontrar_columna(minuta, "delivery")

    # Identificar columnas clave en CI
    col_desc_ci = encontrar_columna(ci, "des no custom")
    col_qty_ci = encontrar_columna(ci, "delivery quantity")

    # Normalizar valores
    minuta[col_desc_minuta] = minuta[col_desc_minuta].astype(str).str.strip().str.lower()
    ci[col_desc_ci] = ci[col_desc_ci].astype(str).str.strip().str.lower()

    # Asegurar columnas extra en CI
    if 'net price' not in ci.columns:
        ci['net price'] = None
    if 'commodity code3' not in ci.columns:
        ci['commodity code3'] = None

    ci['linea tipo'] = None

    minuta_saldos = minuta.copy()
    result_ci = []
    consumos = []  # Para desglose adicional

    # === Lógica FIFO por línea de CI ===
    for _, row in ci.iterrows():
        producto = row[col_desc_ci]
        qty_needed = row[col_qty_ci]
        partes_ci = []

        # Filtrar minuta para ese producto (FIFO)
        filtro_producto = minuta_saldos[minuta_saldos[col_desc_minuta] == producto]

        for i_m, fila_minuta in filtro_producto.iterrows():
            saldo_actual = minuta_saldos.at[i_m, col_saldo_minuta]
            if saldo_actual <= 0 or qty_needed <= 0:
                continue

            # Saldo inicial para registro de consumo
            saldo_inicial = saldo_actual

            if qty_needed <= saldo_actual:
                # Consumir solo lo necesario
                minuta_saldos.at[i_m, col_saldo_minuta] -= qty_needed

                nueva_linea = row.copy()
                nueva_linea['delivery quantity'] = qty_needed
                nueva_linea['commodity code'] = fila_minuta[col_fraccion]
                nueva_linea['commodity code3'] = fila_minuta[col_desc_fraccion]
                nueva_linea['net price'] = fila_minuta[col_precio]
                nueva_linea['linea tipo'] = 'línea de sse'
                partes_ci.append(nueva_linea)

                # Registrar consumo
                consumos.append({
                    'Delivery': fila_minuta[col_delivery_minuta],
                    'Descripcion': fila_minuta[col_desc_minuta],
                    'Fraccion': fila_minuta[col_fraccion],
                    'Desc Fraccion': fila_minuta[col_desc_fraccion],
                    'Precio Unitario': fila_minuta[col_precio],
                    'Cantidad Descontada': qty_needed,
                    'Saldo Inicial': saldo_inicial,
                    'Saldo Final': saldo_inicial - qty_needed
                })

                qty_needed = 0
                break
            else:
                # Consumir todo el saldo y seguir
                minuta_saldos.at[i_m, col_saldo_minuta] = 0

                nueva_linea_sse = row.copy()
                nueva_linea_sse['delivery quantity'] = saldo_actual
                nueva_linea_sse['commodity code'] = fila_minuta[col_fraccion]
                nueva_linea_sse['commodity code3'] = fila_minuta[col_desc_fraccion]
                nueva_linea_sse['net price'] = fila_minuta[col_precio]
                nueva_linea_sse['linea tipo'] = 'línea de sse'
                partes_ci.append(nueva_linea_sse)

                # Registrar consumo parcial
                consumos.append({
                    'Delivery': fila_minuta[col_delivery_minuta],
                    'Descripcion': fila_minuta[col_desc_minuta],
                    'Fraccion': fila_minuta[col_fraccion],
                    'Desc Fraccion': fila_minuta[col_desc_fraccion],
                    'Precio Unitario': fila_minuta[col_precio],
                    'Cantidad Descontada': saldo_actual,
                    'Saldo Inicial': saldo_inicial,
                    'Saldo Final': 0
                })

                qty_needed -= saldo_actual

        # Si faltó cantidad → Maquila
        if qty_needed > 0:
            nueva_linea_maquila = row.copy()
            nueva_linea_maquila['delivery quantity'] = qty_needed
            nueva_linea_maquila['linea tipo'] = 'línea de maquila'
            partes_ci.append(nueva_linea_maquila)

        # Marcar fraccionada
        if len(partes_ci) > 1:
            for frag in partes_ci:
                frag['Fraccionada'] = 'Sí'
        else:
            for frag in partes_ci:
                frag['Fraccionada'] = 'No'

        result_ci.extend(partes_ci)

    # === Resultado CI modificado ===
    ci_modificado = pd.DataFrame(result_ci)

    # Ordenar por Document + Item
    if 'document' in ci_modificado.columns and 'item' in ci_modificado.columns:
        ci_modificado = ci_modificado.sort_values(by=['document', 'item']).reset_index(drop=True)

    # === Resultado Minuta actualizada ===
    minuta_actualizada = minuta_saldos

    # === Hoja de consumos ===
    consumos_df = pd.DataFrame(consumos)

    return ci_modificado, minuta_actualizada, consumos_df


# ==============================
# INTERFAZ STREAMLIT
# ==============================
st.set_page_config(page_title="FIFO Minuta-CI", layout="wide")
st.title("🔄 Análisis FIFO Minuta vs CI")

st.write("Sube un archivo con las hojas **Minuta** y **CI** para procesar el análisis FIFO y generar los archivos resultantes:")

file = st.file_uploader("Cargar archivo Excel", type=["xlsx"])

if file:
    # Procesar
    ci_modificado, minuta_actualizada, consumos_df = procesar_fifo(file)

    st.success("¡Procesamiento completado!")

    # Vista previa
    st.subheader("Vista previa - CI modificado")
    st.dataframe(ci_modificado.head(30))

    st.subheader("Vista previa - Minuta actualizada")
    st.dataframe(minuta_actualizada.head(30))

    st.subheader("Vista previa - Desglose de consumos")
    st.dataframe(consumos_df.head(30))

    # Función exportar Excel con dos hojas
    def export_excel_minuta(minuta_df, consumos_df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            minuta_df.to_excel(writer, index=False, sheet_name="Minuta Actualizada")
            consumos_df.to_excel(writer, index=False, sheet_name="Consumos")
        return output.getvalue()

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
        label="📥 Descargar Minuta (con consumos)",
        data=export_excel_minuta(minuta_actualizada, consumos_df),
        file_name="Minuta_con_consumos.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
