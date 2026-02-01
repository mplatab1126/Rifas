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
 * Inicializa solo RANDOM (E2:E51)
 */
function inicializarListaRandom(){
  const hojaNumeros = n_getSheet("NUMEROS");
  n_getSheet("NUMEROS CHATEA"); 

  const last = hojaNumeros.getLastRow();
  if(last<2){ 
    n_clearAndFillColumn(hojaNumeros, 5, 2, 50, []); // E2:E51
    actualizarOrganizados();
    return; 
  }

  // Filtra SOLO disponibles (no VENDIDO)
  const datos = hojaNumeros.getRange(2,1,last-1,2).getValues(); 
  const disponibles = datos
    .filter(([n,estado]) => n && String(estado||"").trim()!=="VENDIDO")
    .map(([n])=>n_pad4(n));

  n_clearAndFillColumn(hojaNumeros, 5, 2, 50, []); // Limpiar E

  if(disponibles.length === 0){
    actualizarOrganizados();
    Logger.log("⚠️ No hay números disponibles.");
    return;
  }

  // RANDOM (E): hasta 50 únicos
  const takeE = Math.min(50, disponibles.length);
  const r1 = n_pickUniqueRandom(disponibles, takeE).sort((a,b)=>Number(a)-Number(b));

  // Escribir
  n_clearAndFillColumn(hojaNumeros, 5, 2, 50, r1); // E
  
  // LIMPIEZA DE LA COLUMNA F (Por si acaso queda basura antigua)
  hojaNumeros.getRange(2, 6, 50, 1).clearContent(); 

  actualizarOrganizados();
  Logger.log(`✅ RANDOM inicializado con ${r1.length} números.`);
}

// ======================================================
// ACTUALIZADOR DE INVENTARIO (CON RESET)
// ======================================================

function actualizarEstadoNumeros() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const shNumeros = ss.getSheetByName("NUMEROS");
  
  if (!shNumeros) { console.error("No se encontró la hoja NUMEROS"); return; }

  // 1. OBTENER LISTA DE NÚMEROS (Columna A)
  const lastRow = shNumeros.getLastRow();
  if (lastRow < 2) return;
  
  // Leemos los números (Col A)
  const rangoNumeros = shNumeros.getRange(2, 1, lastRow - 1, 1).getValues().flat();
  const listaNumeros = rangoNumeros.map(n => Number(n));

  // 2. RECOLECTAR NÚMEROS VENDIDOS DE TODAS LAS HOJAS
  // CORREGIDO: Listas nuevas
  const listaVentas = (typeof VENTAS_SHARDS !== 'undefined' ? VENTAS_SHARDS : ["V1", "V2"]);
  const listaVarias = (typeof VARIAS_SHARDS !== 'undefined' ? VARIAS_SHARDS : [
    "VB1", "VB2", "VB3", "VB4", "VB5", "VB6", "VB7", "VB8", "VB9", "VB10"
  ]);
  
  const hojasBusqueda = listaVentas.concat(listaVarias);
  
  let vendidosSet = new Set();

  hojasBusqueda.forEach(nombreHoja => {
    const sh = ss.getSheetByName(nombreHoja);
    if (sh && sh.getLastRow() > 1) {
      // EN LA NUEVA ESTRUCTURA, EL NÚMERO DE BOLETA ES LA COLUMNA B (2)
      const datos = sh.getRange(2, 2, sh.getLastRow() - 1, 1).getValues().flat();
      datos.forEach(d => {
        let valor = Number(d);
        if (!isNaN(valor) && valor !== 0) {
          vendidosSet.add(valor);
        }
      });
    }
  });

  // 3. PREPARAR LOS CAMBIOS (ESTADO Y COLOR)
  const estados = [];
  const colores = [];

  for (let i = 0; i < listaNumeros.length; i++) {
    let numeroActual = listaNumeros[i];
    
    if (vendidosSet.has(numeroActual)) {
      // Si está en la lista de vendidos
      estados.push(["VENDIDO"]);
      colores.push(["#ea9999"]); // Rojo claro
    } else {
      // Si NO está vendido (Reset a disponible)
      estados.push(["DISPONIBLE"]);
      colores.push(["#ffffff"]); // Blanco
    }
  }

  // 4. ESCRIBIR EN LA HOJA DE UNA SOLA VEZ (OPTIMIZADO)
  shNumeros.getRange(2, 2, estados.length, 1).setValues(estados);
  shNumeros.getRange(2, 1, colores.length, 1).setBackgrounds(colores);

  console.log("✅ Inventario actualizado correctamente.");
}