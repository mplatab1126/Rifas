// ======================================================
// ROBOT DE LIMPIEZA DE DUPLICADOS (ANTI-ZOMBIES)
// ======================================================

function limpiarZombiesDiario() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  
  // 1. Recolectar todos los números que YA están en "VARIAS BOLETAS" (Destino seguro)
  let numerosEnVarias = new Set();
  
  // CORREGIDO: Usamos las listas actualizadas
  const hojasVarias = (typeof VARIAS_SHARDS !== 'undefined') ? VARIAS_SHARDS : [
    "VB1", "VB2", "VB3", "VB4", "VB5", 
    "VB6", "VB7", "VB8", "VB9", "VB10"
  ];
  const hojasVentas = (typeof VENTAS_SHARDS !== 'undefined') ? VENTAS_SHARDS : ["V1", "V2"];

  // Llenar el registro de "Varias"
  hojasVarias.forEach(nombre => {
    const sh = ss.getSheetByName(nombre);
    if(sh && sh.getLastRow() > 1) {
      // En VB, la boleta está en columna B (2)
      const datos = sh.getRange(2, 2, sh.getLastRow()-1, 1).getValues().flat();
      datos.forEach(n => numerosEnVarias.add(Number(n)));
    }
  });

  if (numerosEnVarias.size === 0) {
    console.log("No hay datos en Varias Boletas (VB). Nada que limpiar.");
    return;
  }

  // 2. Buscar intrusos en "VENTAS" (Origen)
  // Si un número está en "Varias", NO debería estar en "Ventas". Si está, es un Zombie.
  
  let eliminados = 0;

  hojasVentas.forEach(nombre => {
    const sh = ss.getSheetByName(nombre);
    if(sh && sh.getLastRow() > 1) {
      // Leemos de abajo hacia arriba para poder borrar filas sin dañar el índice
      const ultimaFila = sh.getLastRow();
      
      // En V1/V2, la boleta está en columna B (2)
      const datos = sh.getRange(2, 2, ultimaFila-1, 1).getValues().flat();
      
      // Recorremos inversamente
      for (let i = datos.length - 1; i >= 0; i--) {
        const numeroEnVenta = Number(datos[i]);
        const filaReal = i + 2; // +2 porque el array empieza en 0 y hay encabezado

        // LA PRUEBA DEL ZOMBIE:
        if (numerosEnVarias.has(numeroEnVenta)) {
          // ¡Encontrado! Está en VB y también aquí en V. Borrar de aquí.
          console.log(`🧟 Zombie detectado: Boleta ${numeroEnVenta} en hoja "${nombre}". Eliminando...`);
          sh.deleteRow(filaReal);
          eliminados++;
        }
      }
    }
  });

  if (eliminados > 0) {
    _registrarAuditoria("LIMPIEZA AUTOMÁTICA", "SISTEMA", "ROBOT", `Se eliminaron ${eliminados} registros duplicados (zombies) de Ventas.`);
  }
  
  console.log(`✅ Limpieza terminada. Zombies eliminados: ${eliminados}`);
}