// ------------------------------------------------------
// Archivo: numeros.gs  (Gestión de una sola lista RANDOM)
// - Marca VENDIDO en NUMEROS según boletas de todas las hojas de VENTAS
// - Mantiene RANDOM (E2:E51) actualizado y sin vendidos
// - Actualiza "NUMEROS CHATEA" (B2) con lo disponible
// ------------------------------------------------------

// Fallback SEGURO: Actualizado a V1/V2
(function (g) {
  g.VENTAS_SHARDS = g.VENTAS_SHARDS || ["V1", "V2"];
})(this);

/* =============== Helpers mínimos (prefijo n_) =============== */
function n_getSS(){ return SpreadsheetApp.openById(SPREADSHEET_ID); }
function n_getSheet(name){ const sh=n_getSS().getSheetByName(name); if(!sh) throw new Error(`No existe la hoja "${name}"`); return sh; }
function n_pad4(v){ const s=String(v==null?"":v).trim(); return ("0000"+s).slice(-4); }

/** Devuelve las hojas de ventas que existan en el libro. */
function n_getAllVentaSheets(){
  const ss = n_getSS();
  const names = ss.getSheets().map(s=>s.getName());
  const out = [];

  // 1. Definimos todas las listas donde puede haber ventas
  // CORREGIDO: Nombres nuevos V1, V2 y VB1...VB10
  const listasA = (typeof VENTAS_SHARDS !== 'undefined') ? VENTAS_SHARDS : ["V1", "V2"];
  const listasB = (typeof VARIAS_SHARDS !== 'undefined') ? VARIAS_SHARDS : [
    "VB1", "VB2", "VB3", "VB4", "VB5", 
    "VB6", "VB7", "VB8", "VB9", "VB10"
  ];
  
  const todas = [...listasA, ...listasB];

  // 2. Buscamos las hojas reales
  for(const name of todas){
    if (names.includes(name)) out.push(ss.getSheetByName(name));
  }
  return out;
}

/** Baraja (Fisher–Yates) y devuelve los primeros count elementos únicos. */
function n_pickUniqueRandom(arr, count){
  const a = arr.slice(); // copia
  for (let i=a.length-1; i>0; i--){
    const j = Math.floor(Math.random() * (i+1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, count);
}

/** Limpia y rellena una columna (maxRows) desde startRow con values (arr 1D). */
function n_clearAndFillColumn(sh, col, startRow, maxRows, values){
  const total = maxRows;
  const out = Array.from({length: total}, (_, i) => [ values[i] ?? "" ]);
  sh.getRange(startRow, col, total, 1).setValues(out);
}

/* =============== Núcleo: cruzar VENTAS->NUMEROS =============== */
function revisarVentasAutomaticamente(){
  const ss            = n_getSS();
  const hojaNumeros = ss.getSheetByName("NUMEROS");
  if(!hojaNumeros){ Logger.log("⛔ Falta la hoja NUMEROS"); return; }

  // Cache de NUMEROS (A: número, B: estado)
  const lastN = hojaNumeros.getLastRow();
  if(lastN < 2){ Logger.log("⚠️ NUMEROS está vacío."); return; }
  const numerosData = hojaNumeros.getRange(2,1,lastN-1,2).getValues(); // [[num,estado],...]
  const mapIndicePorNumero = new Map(); 
  for(let i=0;i<numerosData.length;i++){
    const n = n_pad4(numerosData[i][0]);
    if(n) mapIndicePorNumero.set(n,i);
  }

  const ventaSheets = n_getAllVentaSheets();
  if(ventaSheets.length===0){ Logger.log("⚠️ No hay hojas VENTAS"); return; }

  let cambios = 0;
  for(const sh of ventaSheets){
    const last = sh.getLastRow();
    if(last<2) continue;

    // IMPORTANTE: Busca la columna por NOMBRE. Asegúrate de que las hojas VB tengan encabezado.
    const headers = sh.getRange(1,1,1, sh.getLastColumn()).getValues()[0];
    const colBoleta = headers.indexOf("NUMERO BOLETA")+1;
    
    if(!colBoleta){ 
        // Fallback: Si no encuentra el encabezado, asume Columna B (2) como definimos en Code.gs
        // Logger.log(`⚠️ En "${sh.getName()}" no encontré columna "NUMERO BOLETA". Usando Columna B.`); 
        // const valores = sh.getRange(2, 2, last-1, 1).getValues().flat();
        continue; 
    }

    const valores = sh.getRange(2,colBoleta,last-1,1).getValues().flat();
    for(const v of valores){
      if(v==null || v==="") continue;
      const clave = n_pad4(v);
      const idx = mapIndicePorNumero.get(clave);
      if(idx==null) continue;

      const estadoActual = String(numerosData[idx][1]||"").trim();
      if(estadoActual !== "VENDIDO"){
        hojaNumeros.getRange(idx+2, 2).setValue("VENDIDO"); // B
        hojaNumeros.getRange(idx+2, 1).setBackground("#ea9999"); // A rojo suave
        // Reemplazar en RANDOM si aparece
        reemplazarNumeroEnRandom(clave);
        cambios++;
      }
    }
  }

  if(cambios>0){
    actualizarOrganizados();
    Logger.log(`✅ Revisado. Cambios aplicados: ${cambios}`);
  }else{
    Logger.log("✔️ Sin cambios.");
  }
}

/* =============== RANDOM y ORGANIZADOS (SOLO COLUMNA E) =============== */
function reemplazarNumeroEnRandom(numeroVendido){
  if(numeroVendido==null) return;
  const strVend = n_pad4(numeroVendido);

  const hoja = n_getSheet("NUMEROS");
  const last = hoja.getLastRow(); if(last<2) return;

  const datos   = hoja.getRange(2,1,last-1,2).getValues(); // A: num, B: estado
  const colE    = hoja.getRange(2,5,50,1).getValues().flat(); // RANDOM E2:E51
  const randoms = colE.map(n=>n_pad4(n));

  // disponibles: no VENDIDO y que no estén ya en E
  const disponibles = datos
    .filter(([n,estado]) => n && String(estado||"").trim()!=="VENDIDO")
    .map(([n])=>n_pad4(n))
    .filter(n => !randoms.includes(n));

  // Buscar en E
  let idxE = randoms.indexOf(strVend);
  if(idxE>=0){
    const nuevo = disponibles.length ? disponibles[Math.floor(Math.random()*disponibles.length)] : "";
    hoja.getRange(idxE+2, 5).setValue(nuevo); // E fila idx+2
  }
}

/** Refresca texto organizado en "NUMEROS CHATEA" (Solo B2) */
function actualizarOrganizados(){
  const hojaNumeros = n_getSheet("NUMEROS");
  const hojaChatea  = n_getSheet("NUMEROS CHATEA");

  const rand1 = hojaNumeros.getRange(2,5,50,1).getValues().flat()
    .filter(n => n!=null && n!=="")
    .map(n=>n_pad4(n))
    .sort((a,b)=>Number(a)-Number(b));
  
  // Solo escribimos en la lista 1 (Fila 2)
  hojaChatea.getRange("B2").setValue(rand1.join(" - "));
}

/**
 * Inicializa RANDOM (E2:E51) y RANDOM 2 (F2:F51)
 * - Busca números disponibles (no VENDIDO)
 * - Llena Columna E con 50 números
 * - Llena Columna F con OTROS 50 números (sin repetir los de la E)
 */
function inicializarListaRandom(){
  const hojaNumeros = n_getSheet("NUMEROS");
  n_getSheet("NUMEROS CHATEA"); // Asegura que existe la hoja auxiliar

  const last = hojaNumeros.getLastRow();
  
  // Limpiamos las columnas E y F antes de empezar
  n_clearAndFillColumn(hojaNumeros, 5, 2, 50, []); // Limpiar E
  n_clearAndFillColumn(hojaNumeros, 6, 2, 50, []); // Limpiar F

  if(last<2){ 
    actualizarOrganizados();
    return;
  }

  // Filtra SOLO disponibles (no VENDIDO)
  const datos = hojaNumeros.getRange(2,1,last-1,2).getValues();
  const disponibles = datos
    .filter(([n,estado]) => n && String(estado||"").trim()!=="VENDIDO")
    .map(([n])=>n_pad4(n));

  if(disponibles.length === 0){
    actualizarOrganizados();
    Logger.log("⚠️ No hay números disponibles.");
    return;
  }

  // --- CAMBIO CLAVE: Pedimos 100 números de una vez ---
  // Así aseguramos que los de la lista 2 no se repitan con la lista 1
  const totalNecesario = 100;
  const mezclados = n_pickUniqueRandom(disponibles, totalNecesario);

  // Repartimos: 0-49 para RANDOM 1, 50-99 para RANDOM 2
  const r1 = mezclados.slice(0, 50).sort((a,b)=>Number(a)-Number(b));
  const r2 = mezclados.slice(50, 100).sort((a,b)=>Number(a)-Number(b));

  // Escribir en Columna E (5) - RANDOM 1
  if (r1.length > 0) {
    n_clearAndFillColumn(hojaNumeros, 5, 2, 50, r1);
  }

  // Escribir en Columna F (6) - RANDOM 2
  if (r2.length > 0) {
    n_clearAndFillColumn(hojaNumeros, 6, 2, 50, r2);
  }

  // Actualizamos el resumen
  actualizarOrganizados();
  Logger.log(`✅ RANDOM actualizado: ${r1.length} en Col E y ${r2.length} en Col F.`);
}

// ======================================================
// ACTUALIZADOR DE INVENTARIO (CON RESET)
// ======================================================

/** * Refresca texto organizado en "NUMEROS CHATEA":
 * - B2: Lista de la Columna E (Random 1)
 * - B3: Lista de la Columna F (Random 2)
 */
function actualizarOrganizados(){
  const hojaNumeros = n_getSheet("NUMEROS");
  const hojaChatea  = n_getSheet("NUMEROS CHATEA");

  // 1. Procesar LISTA RANDOM 1 (Columna E -> índice 5)
  const rand1 = hojaNumeros.getRange(2, 5, 50, 1).getValues().flat()
    .filter(n => n != null && String(n).trim() !== "")
    .map(n => n_pad4(n))
    .sort((a, b) => Number(a) - Number(b));
  
  // 2. Procesar LISTA RANDOM 2 (Columna F -> índice 6)
  const rand2 = hojaNumeros.getRange(2, 6, 50, 1).getValues().flat()
    .filter(n => n != null && String(n).trim() !== "")
    .map(n => n_pad4(n))
    .sort((a, b) => Number(a) - Number(b));

  // 3. Escribir en la hoja auxiliar
  // Lista 1 en B2
  hojaChatea.getRange("B2").setValue(rand1.join(" - "));
  // Lista 2 en B3
  hojaChatea.getRange("B3").setValue(rand2.join(" - "));
}