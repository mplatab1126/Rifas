(function (g) {
  g.VENTAS_SHARDS = g.VENTAS_SHARDS || ["V1", "V2"];
})(this);

function n_getSS(){ return SpreadsheetApp.openById(SPREADSHEET_ID); }
function n_getSheet(name){ const sh=n_getSS().getSheetByName(name); if(!sh) throw new Error(`No existe la hoja "${name}"`); return sh; }
function n_pad4(v){ const s=String(v==null?"":v).trim(); return ("0000"+s).slice(-4); }

function n_getAllVentaSheets(){
  const ss = n_getSS();
  const names = ss.getSheets().map(s=>s.getName());
  const out = [];
  const listasA = (typeof VENTAS_SHARDS !== 'undefined') ? VENTAS_SHARDS : ["V1", "V2"];
  const listasB = (typeof VARIAS_SHARDS !== 'undefined') ? VARIAS_SHARDS : [
    "VB1", "VB2", "VB3", "VB4", "VB5", 
    "VB6", "VB7", "VB8", "VB9", "VB10"
  ];
  const todas = [...listasA, ...listasB];
  for(const name of todas){
    if (names.includes(name)) out.push(ss.getSheetByName(name));
  }
  return out;
}

function n_pickUniqueRandom(arr, count){
  const a = arr.slice();
  for (let i=a.length-1; i>0; i--){
    const j = Math.floor(Math.random() * (i+1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, count);
}

function n_clearAndFillColumn(sh, col, startRow, maxRows, values){
  const total = maxRows;
  const out = Array.from({length: total}, (_, i) => [ values[i] ?? "" ]);
  sh.getRange(startRow, col, total, 1).setValues(out);
}

function revisarVentasAutomaticamente(){
  const ss = n_getSS();
  const hojaNumeros = ss.getSheetByName("NUMEROS");
  if(!hojaNumeros){ Logger.log("⛔ Falta la hoja NUMEROS"); return; }

  const lastN = hojaNumeros.getLastRow();
  if(lastN < 2){ Logger.log("⚠️ NUMEROS está vacío."); return; }
  const numerosData = hojaNumeros.getRange(2,1,lastN-1,2).getValues();
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
    const headers = sh.getRange(1,1,1, sh.getLastColumn()).getValues()[0];
    const colBoleta = headers.indexOf("NUMERO BOLETA")+1;
    if(!colBoleta) continue; 

    const valores = sh.getRange(2,colBoleta,last-1,1).getValues().flat();
    for(const v of valores){
      if(v==null || v==="") continue;
      const clave = n_pad4(v);
      const idx = mapIndicePorNumero.get(clave);
      if(idx==null) continue;

      const estadoActual = String(numerosData[idx][1]||"").trim();
      if(estadoActual !== "VENDIDO"){
        hojaNumeros.getRange(idx+2, 2).setValue("VENDIDO");
        hojaNumeros.getRange(idx+2, 1).setBackground("#ea9999");
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

function reemplazarNumeroEnRandom(numeroVendido){
  if(numeroVendido==null) return;
  const strVend = n_pad4(numeroVendido);
  const hoja = n_getSheet("NUMEROS");
  const last = hoja.getLastRow(); if(last<2) return;
  const datos   = hoja.getRange(2,1,last-1,2).getValues();
  const colE    = hoja.getRange(2,5,50,1).getValues().flat();
  const randoms = colE.map(n=>n_pad4(n));
  const disponibles = datos
    .filter(([n,estado]) => n && String(estado||"").trim()!=="VENDIDO")
    .map(([n])=>n_pad4(n))
    .filter(n => !randoms.includes(n));
  let idxE = randoms.indexOf(strVend);
  if(idxE>=0){
    const nuevo = disponibles.length ?
    disponibles[Math.floor(Math.random()*disponibles.length)] : "";
    hoja.getRange(idxE+2, 5).setValue(nuevo);
  }
}

function actualizarOrganizados(){
  const hojaNumeros = n_getSheet("NUMEROS");
  const hojaChatea  = n_getSheet("NUMEROS CHATEA");
  const rand1 = hojaNumeros.getRange(2, 5, 50, 1).getValues().flat()
    .filter(n => n != null && String(n).trim() !== "")
    .map(n => n_pad4(n))
    .sort((a, b) => Number(a) - Number(b));
  const rand2 = hojaNumeros.getRange(2, 6, 50, 1).getValues().flat()
    .filter(n => n != null && String(n).trim() !== "")
    .map(n => n_pad4(n))
    .sort((a, b) => Number(a) - Number(b));
  hojaChatea.getRange("B2").setValue(rand1.join(" - "));
  hojaChatea.getRange("B3").setValue(rand2.join(" - "));
}

function inicializarListaRandom(){
  const hojaNumeros = n_getSheet("NUMEROS");
  n_getSheet("NUMEROS CHATEA"); 
  const last = hojaNumeros.getLastRow();
  n_clearAndFillColumn(hojaNumeros, 5, 2, 50, []);
  n_clearAndFillColumn(hojaNumeros, 6, 2, 50, []);
  if(last<2){ 
    actualizarOrganizados();
    return;
  }
  const datos = hojaNumeros.getRange(2,1,last-1,2).getValues();
  const disponibles = datos
    .filter(([n,estado]) => n && String(estado||"").trim()!=="VENDIDO")
    .map(([n])=>n_pad4(n));
  if(disponibles.length === 0){
    actualizarOrganizados();
    Logger.log("⚠️ No hay números disponibles.");
    return;
  }
  const totalNecesario = 100;
  const mezclados = n_pickUniqueRandom(disponibles, totalNecesario);
  const r1 = mezclados.slice(0, 50).sort((a,b)=>Number(a)-Number(b));
  const r2 = mezclados.slice(50, 100).sort((a,b)=>Number(a)-Number(b));
  if (r1.length > 0) {
    n_clearAndFillColumn(hojaNumeros, 5, 2, 50, r1);
  }
  if (r2.length > 0) {
    n_clearAndFillColumn(hojaNumeros, 6, 2, 50, r2);
  }
  actualizarOrganizados();
  Logger.log(`✅ RANDOM actualizado: ${r1.length} en Col E y ${r2.length} en Col F.`);
}