/**
 * NOMBRES DE TUS HOJAS
 * Aquí están los nombres corregidos.
 */
var HOJA_DE_DATOS_VIVOS = "HISTORICO DIARIO"; // La hoja con las fórmulas (la de tu foto)
var HOJA_DE_REGISTRO = "LOG_DIARIO";       // La hoja nueva donde se guarda el registro

/**
 * Esta función se ejecuta automáticamente (usando un activador)
 * para tomar una "foto" de los totales del día.
 */
function tomarFotoDiaria() {
  try {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var hojaDatos = spreadsheet.getSheetByName(HOJA_DE_DATOS_VIVOS);
    var hojaRegistro = spreadsheet.getSheetByName(HOJA_DE_REGISTRO);
    
    if (!hojaDatos) {
      console.error("Error en tomarFotoDiaria: No se encontró la hoja: " + HOJA_DE_DATOS_VIVOS);
      return;
    }
    if (!hojaRegistro) {
      console.error("Error en tomarFotoDiaria: No se encontró la hoja: " + HOJA_DE_REGISTRO);
      return;
    }

    // Obtener la fecha de hoy. Usamos GMT-5 (zona horaria de Colombia)
    var hoy = new Date();
    
    // Formatear la fecha de hoy para que coincida con tu Columna A (ej. "28-oct-2025")
    // Usamos toLowerCase() para que "Oct" se vuelva "oct"
    var fechaDeHoyFormateada = Utilities.formatDate(hoy, "GMT-5", "dd-MMM-yyyy").toLowerCase(); 
    
    // Buscar la fila de hoy en la hoja de datos vivos (HISTORICO DIARIO)
    var rangoFechas = hojaDatos.getRange("A2:A").getValues();
    var filaEncontrada = -1;

    for (var i = 0; i < rangoFechas.length; i++) {
      if (rangoFechas[i][0] && rangoFechas[i][0] != "") {
        // Convertir la fecha de la celda a un string comparable
        var fechaCelda = new Date(rangoFechas[i][0]);
        var fechaCeldaFormateada = Utilities.formatDate(fechaCelda, "GMT-5", "dd-MMM-yyyy").toLowerCase();

        if (fechaCeldaFormateada == fechaDeHoyFormateada) {
          filaEncontrada = i + 2; // +2 porque el rango empieza en A2
          break;
        }
      }
    }

    if (filaEncontrada == -1) {
      console.error("Error en tomarFotoDiaria: No se encontró la fila para la fecha: " + fechaDeHoyFormateada + " en la hoja " + HOJA_DE_DATOS_VIVOS);
      return;
    }

    // Leer los valores calculados por las fórmulas de esa fila
    // (B = Total Boletas, C = Con Abono, D = Sin Abono)
    var valores = hojaDatos.getRange(filaEncontrada, 2, 1, 3).getValues();
    var totalBoletas = valores[0][0];
    var conAbono = valores[0][1];
    var sinAbono = valores[0][2];

    // Añadir la "foto" (valores fijos) a la hoja LOG_DIARIO
    // Guardamos la fecha real (hoy) y los 3 valores
    hojaRegistro.appendRow([hoy, totalBoletas, conAbono, sinAbono]);
    
    console.log("Foto del día " + fechaDeHoyFormateada + " guardada exitosamente en " + HOJA_DE_REGISTRO);

  } catch (e) {
    console.error("Error catastrófico en tomarFotoDiaria: " + e.message);
  }
}