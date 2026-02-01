/**
 * Esta función se ejecuta automáticamente cada noche para registrar las métricas de ventas del día anterior.
 * Solo guarda los datos brutos (fecha y conteo de boletas). Las columnas de inversión y cálculos
 * se manejan con fórmulas directamente en la hoja de cálculo.
 */
function registrarDatosDiarios() {
  // Define los nombres de tus hojas
  const SPREADSHEET = SpreadsheetApp.getActiveSpreadsheet();
  const DASHBOARD_SHEET_NAME = "Dashboard Principal";
  const HISTORICO_SHEET_NAME = "Histórico Diario";

  // Accede a las hojas
  const dashboardSheet = SPREADSHEET.getSheetByName(DASHBOARD_SHEET_NAME);
  const historicoSheet = SPREADSHEET.getSheetByName(HISTORICO_SHEET_NAME);

  // 1. CALCULAR LA FECHA DE AYER
  // El script calcula la fecha del día que acaba de terminar.
  const ayer = new Date();
  ayer.setDate(ayer.getDate() - 1);
  
  // 2. ACTUALIZAR EL DASHBOARD CON LA FECHA DE AYER
  // El script pone la fecha de ayer en la celda C2 para forzar el recálculo de las fórmulas.
  dashboardSheet.getRange("C2").setValue(ayer);
  
  // Pausa para asegurar que las fórmulas de la hoja de cálculo tengan tiempo de actualizarse.
  SpreadsheetApp.flush();
  Utilities.sleep(3000); // Espera 3 segundos.

  // 3. LEER LOS DATOS CALCULADOS DEL DASHBOARD
  const fecha = dashboardSheet.getRange("C2").getValue();
  const totalBoletas = dashboardSheet.getRange("C3").getValue();
  const conAbono = dashboardSheet.getRange("C4").getValue();
  const sinAbono = dashboardSheet.getRange("C5").getValue();

  // 4. AÑADIR LOS DATOS A LA TABLA DE HISTÓRICO
  // Agrega una nueva fila solo con los datos básicos. Las otras columnas se calcularán con fórmulas.
  historicoSheet.appendRow([fecha, totalBoletas, conAbono, sinAbono]);
}