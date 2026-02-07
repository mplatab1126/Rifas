const SPREADSHEET_ID = "1bdJ6G7l2Iaqp1e3sp01rEYU5hbpOxWrBlm2br8DRUvE";
const ID_CENTRAL_TRANSFERENCIAS = "1DtwLYhRE_3PN8Sl-5We6Qr9BBF54elBhGMQoGYwG28U";
const BASE_URL = "https://script.google.com/macros/s/AKfycbxzRHo_XcLE-FRWQOSmq2wiM1c4WAYgEBf2vGFhYrSXtpcM7jinaoO_BxtlJpan48P5EQ/exec";
const TICKET_PRICE = 150000;
const MAX_ROWS_PER_SHARD = 3333;

const VENTAS_SHARDS = ["V1", "V2"];
const VARIAS_SHARDS = [
  "VB1", "VB2", "VB3", "VB4", "VB5", 
  "VB6", "VB7", "VB8", "VB9", "VB10"
];

const ABONOS_SHARDS = ["ABONOS 1","ABONOS 2","ABONOS 3"];
const ABONOS_SINGLE_NAME = "ABONOS";
const TRANSFERENCIAS_NAME = "TRANSFERENCIAS";

const ASESOR_CREDENTIALS = {
  "m8a3": "Mateo","r0j5": "Manu R","s14": "Saldarriaga","a2n7": "Anyeli","a9e1": "Alejo",
  "m26": "Nena","l22": "Luisa","s19": "Lili","v261": "Vale","l20": "Arias","a21": "Aleja",
  "of": "Oficina", "j1" : "Jennifer","mo2":"Andres","ca1":"Carlos",
  "web_secure_key": "Página Web"
};

function _toNumber(txt){ const s=String(txt??"").trim(); if(s==="") return null; const n=Number(s); return isNaN(n)?null:n; }
function _normRef(s){ return String(s||"").trim().toLowerCase(); }
function _normAlnum(s){ return String(s||"").toLowerCase().replace(/[^a-z0-9]/g,""); }
function _digits(s){ return String(s||"").replace(/\D+/g,""); }
function _samePhone(a,b){ const A=_digits(a),B=_digits(b); if(!A||!B) return false; return A===B||A.endsWith(B)||B.endsWith(A); }

function _normHora12(s){
  s=String(s||"").trim().toLowerCase().replace(/\./g,"").replace(/\s+/g," ");
  const ampm=s.includes("pm")?"PM":"AM";
  const m=s.match(/(\d{1,2})\s*:\s*(\d{2})/);
  if(!m) return "";
  let hh=("0"+m[1]).slice(-2); const mm=("0"+m[2]).slice(-2);
  if(hh==="00") hh="12";
  return `${hh}:${mm} ${ampm}`;
}

function _fechaDispToISO(s){
  if (s instanceof Date && !isNaN(s)){
    const y=s.getFullYear(),m=("0"+(s.getMonth()+1)).slice(-2),d=("0"+s.getDate()).slice(-2);
    return `${y}-${m}-${d}`;
  }
  s=String(s||"").trim();
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const m=s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
  if(!m) return "";
  const d=("0"+m[1]).slice(-2), mo=("0"+m[2]).slice(-2), y=m[3].length===2?("20"+m[3]):m[3];
  return `${y}-${mo}-${d}`;
}

function _withRetry(fn, attempts=3, baseSleepMs=200){
  let lastErr;
  for (let i=0;i<attempts;i++){
    try{ return fn(); }catch(e){
      lastErr = e;
      if(i < attempts-1) Utilities.sleep(baseSleepMs * Math.pow(2,i));
    }
  }
  throw lastErr;
}

function _getSS(){ return SpreadsheetApp.openById(SPREADSHEET_ID); }
function _getSheet(name){
  const sh=_getSS().getSheetByName(name);
  if(!sh) throw new Error(`No existe la hoja "${name}".`);
  return sh;
}
function _pad4(v){ const s=String(v==null?"":v).trim(); return ("0000"+s).slice(-4); }

function _verificarInventarioFINAL(n){
  const buscado = _pad4(n);
  if (buscado === "0000" && n != 0) return false;
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ss.getSheetByName("NUMEROS");
  if (!sh) return false;
  const last = sh.getLastRow();
  if (last < 2) return false;
  const inventario = sh.getRange(2, 1, last - 1, 1).getDisplayValues().flat();
  for (let i = 0; i < inventario.length; i++) {
    if (_pad4(inventario[i]) === buscado) return true;
  }
  return false;
}

function _getAllBoletaSheets() {
  const nombres = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
  const hojas = [];
  const ss = _getSS();
  for (const nombre of nombres) {
    const sh = ss.getSheetByName(nombre);
    if (sh) hojas.push(sh);
  }
  return hojas;
}

function _getAllTransferSheets(){
  const ssExterna = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
  return ssExterna.getSheets().filter(s => /^TRANSFERENCIAS(\s*\d+)?$/i.test(s.getName()));
}

function _getAllVentaSheets(){ return VENTAS_SHARDS.map(n=>_getSheet(n)); }

function _pickVentaShardForWrite(){
  for(const name of VENTAS_SHARDS){
    const sh=_getSheet(name);
    const dataRows=Math.max(0, sh.getLastRow()-1);
    if(dataRows < MAX_ROWS_PER_SHARD) return sh;
  }
  return null;
}

function _abonosUsesSingleSheet(){ return !!_getSS().getSheetByName(ABONOS_SINGLE_NAME); }

function _getAllAbonoSheets(){
  const ss=_getSS();
  if (_abonosUsesSingleSheet()){
    const s = ss.getSheetByName(ABONOS_SINGLE_NAME);
    if(!s) throw new Error(`No existe la hoja "${ABONOS_SINGLE_NAME}".`);
    return [s];
  }
  return ABONOS_SHARDS.map(n=>{
    const s=ss.getSheetByName(n);
    if(!s) throw new Error(`No existe la hoja "${n}".`);
    return s;
  });
}

function _pickAbonoShardForWrite(){
  const sheets=_getAllAbonoSheets();
  if (sheets.length===1) return sheets[0];
  for(const sh of sheets){
    const dataRows=Math.max(0, sh.getLastRow()-1);
    if(dataRows < MAX_ROWS_PER_SHARD) return sh;
  }
  return null;
}

function copiarFormatoUltimaFila(sheetName){
  const sh=_getSheet(sheetName);
  const lastRow = sh.getLastRow();
  if (lastRow <= 2) return;
  const origen  = sh.getRange(lastRow - 1, 1, 1, sh.getLastColumn());
  const destino = sh.getRange(lastRow, 1, 1, sh.getLastColumn());
  origen.copyTo(destino, SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
}

function handleGetRequest(data){
  const numReq = _toNumber(data.numeroBoleta);
  const telReq = _digits(data.telefono);
  const nsReq  = String(data.nsUsuario || "").trim();
  const resultados = [];

  function buildBoletaObjOptimizado(sheet, rowData, rowIndex){
    const totalAbonosNum = Number(rowData[7]) || 0;
    const restanteNum    = Number(rowData[8]) || 0;
    const numeroOriginal = rowData[1];
    const organizado = String(rowData[0] || ("0000"+numeroOriginal).slice(-4)).trim();
    let url = String(rowData[11] || "").trim();
    if(!url) url = `${BASE_URL}?numero=${organizado}`;
    return {
      numero: ("0000"+numeroOriginal).slice(-4),
      nombre: rowData[3],       
      apellido: rowData[4]||"", 
      telefono: rowData[5],     
      ciudad: rowData[6],       
      totalAbonos: totalAbonosNum,
      restante: restanteNum,
      urlBoleta: url,
      nsUsuario: rowData[2]
    };
  }

  if (numReq !== null){
    for (const sheet of _getAllBoletaSheets()){
      const last = sheet.getLastRow();
      if (last < 2) continue;
      const colBoletas = sheet.getRange(2, 2, last - 1, 1).getValues().flat().map(_toNumber);
      const idx = colBoletas.indexOf(numReq);
      if (idx > -1){
        const rowValues = sheet.getRange(idx + 2, 1, 1, 14).getValues()[0];
        return { status:"encontrado", datos: buildBoletaObjOptimizado(sheet, rowValues, idx + 2) };
      }
    }
    if (_verificarInventarioFINAL(numReq)) {
      return { status: "disponible", numero: numReq };
    } else {
      return { status: "noInventario", numero: numReq };
    }
  }

  if (telReq){
    for (const sheet of _getAllBoletaSheets()){
      const last = sheet.getLastRow();
      if (last < 2) continue;
      const colTels = sheet.getRange(2, 6, last - 1, 1).getDisplayValues().flat().map(_digits);
      for (let i = 0; i < colTels.length; i++) {
        if (colTels[i] === telReq){
           const rowValues = sheet.getRange(i + 2, 1, 1, 14).getValues()[0];
           resultados.push(buildBoletaObjOptimizado(sheet, rowValues, i + 2));
        }
      }
    }
  }

  if (nsReq && !telReq && numReq === null) {
    for (const sheet of _getAllBoletaSheets()){
      const last = sheet.getLastRow();
      if (last < 2) continue;
      const colNS = sheet.getRange(2, 3, last - 1, 1).getDisplayValues().flat();
      for (let i = 0; i < colNS.length; i++) {
        if (String(colNS[i]).trim() === nsReq){
           const rowValues = sheet.getRange(i + 2, 1, 1, 14).getValues()[0];
           resultados.push(buildBoletaObjOptimizado(sheet, rowValues, i + 2));
        }
      }
    }
  }

  if (resultados.length > 0) {
    if (resultados.length === 1) return { status:"encontrado", datos: resultados[0] };
    resultados.sort((a,b)=> Number(a.numero) - Number(b.numero));
    return { status:"multiples", lista: resultados };
  }
  return { status:"noExiste" };
}

function existeBoleta(n){
  const numBuscado = _toNumber(n);
  if (numBuscado === null) return false;
  const todasLasHojas = _getAllBoletaSheets();
  for (const sheet of todasLasHojas){
    const last = sheet.getLastRow();
    if (last < 2) continue;
    const valores = sheet.getRange(2, 2, last - 1, 1).getValues();
    for (let i = 0; i < valores.length; i++) {
       const numEnCelda = _toNumber(valores[i][0]);
       if (numEnCelda === numBuscado) return true;
    }
  }
  return false;
}

function _referenciaAbonoExisteEnAbonos(ref){
  const refNeedle=_normRef(ref); if(!refNeedle) return false;
  const sheets=_getAllAbonoSheets();
  for (const sh of sheets){
    const last=sh.getLastRow(); if(last<2) continue;
    const refs=sh.getRange(2,4,last-1,1).getValues().flat().map(_normRef);
    if (refs.includes(refNeedle)) return true;
  }
  return false;
}

function _findTransferenciaByReferencia(ref){
  const needle = _normAlnum(ref);
  if(!needle) return {found:false};
  const sheets = _getAllTransferSheets();
  if (sheets.length === 0) return {found:false};
  for (const sh of sheets){
    const last = sh.getLastRow();
    if (last < 2) continue;
    const vals = sh.getRange(2,1,last-1,7).getDisplayValues();
    for (let i=0;i<vals.length;i++){
      const referencia = vals[i][3];
      if (_normAlnum(referencia) === needle){
        const row = i+2;
        const plataforma = vals[i][1];
        const montoStr   = vals[i][2];
        const fechaDisp  = vals[i][4];
        const horaDisp   = vals[i][5];
        const status     = String(vals[i][6]||"").trim();
        return {
          found:true, sheet: sh.getName(), row,
          referencia, plataforma,
          monto: Number(String(montoStr||"").replace(/[^\d]/g,""))||0,
          fecha: fechaDisp, hora: horaDisp, status
        };
      }
    }
  }
  return {found:false};
}

function _transferenciaYaAsignada(ref){
  const t = _findTransferenciaByReferencia(ref);
  if (!t.found) return false;
  return String(t.status||"").toLowerCase().startsWith("asignado");
}

function validarVentaYRegistrar(data){
  try{
    const pwd=String(data.contrasena||"").trim();
    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    const asesorName=ASESOR_CREDENTIALS[pwd];
    const TELEFONO_CLIENTE = String(data.telefono||"").trim();
    const NS_USUARIO = String(data.nsUsuario || "").trim();
    if (NS_USUARIO === "") return {status: "error", mensaje: "El campo 'NS DEL USUARIO' es obligatorio."};

    const metodoValidacion = String(data.metodoPago||"").trim();
    if (!metodoValidacion || metodoValidacion === "" || metodoValidacion === "Selecciona...") {
       return {status:"error", mensaje:"El campo 'Método de pago' es obligatorio."};
    }

    const num=_toNumber(data.numeroBoleta); 
    if(num===null) return {status:"error",mensaje:"Boleta inválida."};
    if (!_verificarInventarioFINAL(num)) return {status:"error", mensaje:`El número ${data.numeroBoleta} NO pertenece a tu inventario autorizado.`};
    if (existeBoleta(num)) return {status:"duplicada",mensaje:`La boleta ${data.numeroBoleta} ya fue vendida.`};

    const m0=Number(data.primerAbono)||0; 
    const refPrimerAbono=String(data.referenciaAbono||"").trim();
    const refEsEfectivo = _normRef(refPrimerAbono) === "efectivo";
    const refProvista   = !!refPrimerAbono && !refEsEfectivo;

    if (refProvista){
      const tInfo = _findTransferenciaByReferencia(refPrimerAbono);
      if (tInfo.found) {
        const statusLower = String(tInfo.status||"").toLowerCase();
        if (statusLower.startsWith("asignado")) {
           if (!statusLower.includes(TELEFONO_CLIENTE)) {
              return {status:"error", mensaje:`La referencia "${refPrimerAbono}" ya está ASIGNADA y no puede reutilizarse.`};
           }
        }
      }
    }

    if(m0 > TICKET_PRICE) return {status:"error",mensaje:`El primer abono no puede superar ${TICKET_PRICE}.`};
    const sheetA = _pickAbonoShardForWrite();
    const sheetV_Check = _pickVentaShardForWrite();
    if(!sheetA || !sheetV_Check) return {status:"error",mensaje:"No hay hojas disponibles (Ventas o Abonos llenas)."};

    const now = new Date();
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema muy ocupado, intenta de nuevo."};
    try{
      if (existeBoleta(num)) {
         return {status:"duplicada", mensaje:`¡Lo siento! Alguien acaba de comprar la boleta ${data.numeroBoleta} hace un instante.`};
      }

      let sheetV = null;
      let ventaPreviaMover = null; 
      let hojaOrigenPrevia = null;
      let filaOrigenPrevia = 0;
      let existeEnVentas = false;

      for (const nombreHoja of VENTAS_SHARDS) {
        const sh = _getSheet(nombreHoja);
        const last = sh.getLastRow();
        if (last < 2) continue;
        const tels = sh.getRange(2, 6, last - 1, 1).getDisplayValues().flat().map(_digits);
        const idx = tels.indexOf(_digits(TELEFONO_CLIENTE));
        if (idx > -1) {
          existeEnVentas = true;
          hojaOrigenPrevia = sh;
          filaOrigenPrevia = idx + 2;
          ventaPreviaMover = sh.getRange(filaOrigenPrevia, 1, 1, 12).getValues()[0];
          break;
        }
      }

      let conteoVarias = 0;
      const telTarget = _digits(TELEFONO_CLIENTE);
      
      for (const nombreHoja of VARIAS_SHARDS) {
        const sh = _getSS().getSheetByName(nombreHoja);
        if (!sh) continue; 
        const last = sh.getLastRow();
        if (last < 2) continue;
        const datosHoja = sh.getRange(2, 1, last - 1, 6).getValues();
        const encontradosEnHoja = datosHoja.filter(fila => {
           const telFila = _digits(fila[5]); 
           const matchTel = (telTarget.length > 6 && telFila === telTarget);
           return matchTel;
        }).length;
        conteoVarias += encontradosEnHoja;
      }

      if (existeEnVentas) {
        sheetV = _getSheet(VARIAS_SHARDS[1]);
      } else if (conteoVarias > 0) {
        let indiceDestino = conteoVarias % VARIAS_SHARDS.length;
        sheetV = _getSheet(VARIAS_SHARDS[indiceDestino]);
      } else {
        sheetV = _pickVentaShardForWrite();
      }

      if(!sheetV) return {status:"error",mensaje:"No hay hojas de VENTAS/VARIAS disponibles."};

      if (existeEnVentas && ventaPreviaMover) {
        const shDestinoVieja = _getSheet(VARIAS_SHARDS[0]);
        const nuevaFilaVieja = shDestinoVieja.getLastRow() + 1;
        const datosLimpios = ventaPreviaMover.slice(1);
        
        _withRetry(()=> shDestinoVieja.getRange(nuevaFilaVieja, 2, 1, datosLimpios.length).setValues([datosLimpios]));
        _withRetry(()=> hojaOrigenPrevia.deleteRow(filaOrigenPrevia));
        copiarFormatoUltimaFila(shDestinoVieja.getName()); 
        _inyectarFormulas(shDestinoVieja, nuevaFilaVieja, ventaPreviaMover[4] || 0);
      }

      if(m0 > 0){
        const estadoNota = data.esPendiente ? "PENDIENTE" : "";
        _withRetry(()=> sheetA.appendRow([
          num, m0, now, refPrimerAbono, data.metodoPago||"", estadoNota, asesorName
        ]));
        if (refProvista) {
           const infoTrans = _findTransferenciaByReferencia(refPrimerAbono);
           if (infoTrans.found) {
             const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
             const sheetCentral = ssCentral.getSheetByName(infoTrans.sheet);
             const marca = `Asignado - APARMENT - ${TELEFONO_CLIENTE}`;
             _withRetry(()=> sheetCentral.getRange(infoTrans.row, 7).setValue(marca));
           }
        }
        SpreadsheetApp.flush(); 
        Utilities.sleep(200);
      }

      const datosVentaNueva = [
        num, NS_USUARIO, _capitalizar(data.nombre), _capitalizar(data.apellido),
        TELEFONO_CLIENTE, _capitalizar(data.ciudad), 0, 0,
        asesorName, now, "", String(data.metodoPago||"").trim(), String(data.referencia||"").trim()
      ];
      const newRow = sheetV.getLastRow() + 1;
      _withRetry(()=> sheetV.getRange(newRow, 2, 1, 13).setValues([datosVentaNueva]));

      const padded=("0000"+num).slice(-4);
      const url=`${BASE_URL}?numero=${padded}`;
      _withRetry(()=> sheetV.getRange(newRow, 12).setValue(url));

      copiarFormatoUltimaFila(sheetV.getName());
      _inyectarFormulas(sheetV, newRow);
      _marcarVendidoEnInventario(num);

      SpreadsheetApp.flush();
    } finally {
      lock.releaseLock();
    }
    return {status:"ok"};
  }catch(err){
    return {status:"error", mensaje:`Error en el servidor (VENTA): ${String(err.message||err)}`};
  }
}

function _marcarVendidoEnInventario(numero){
  try {
    const ss = _getSS();
    const sh = ss.getSheetByName("NUMEROS");
    if (!sh) return;
    const last = sh.getLastRow(); 
    if (last < 2) return;
    const listaNumeros = sh.getRange(2, 1, last - 1, 1).getValues().flat().map(_toNumber);
    const target = _toNumber(numero);
    const idx = listaNumeros.indexOf(target);
    if (idx > -1) {
      const celdaEstado = sh.getRange(idx + 2, 2);
      celdaEstado.setValue("VENDIDO");
    }
  } catch(e) {
    console.error("Error marcando inventario: " + e.message);
  }
}

function listarAbonosDeNumero(payload){
  try{
    const numero = (typeof payload === 'object') ? _toNumber(payload?.numero) : _toNumber(payload);
    if(numero===null) return {status:"ok", lista:[]};
    const lista=_listAbonos(numero);
    const fmt = (v)=> {
      if (v instanceof Date && !isNaN(v)) {
        const d = Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
        const h = Utilities.formatDate(v, Session.getScriptTimeZone(), "hh:mm a");
        return {fecha:d, hora:h};
      }
      const iso = _fechaDispToISO(v);
      return {fecha: iso||"", hora:""};
    };
    const out = lista.map(a=>{
      const f=fmt(a.fechaHora);
      return {
        sheet:a.sheet, row:a.row, numero:a.numero,
        fecha:f.fecha, hora:f.hora, valor:a.valor,
        referencia:a.referencia, metodo:a.metodo
      };
    });
    return {status:"ok", lista: out};
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}
function listarAbonosPorNumero(payload){ return listarAbonosDeNumero(payload); }

function validarAbonoYRegistrar(data){
  try{
    const pwd = String(data?.contrasena||"").trim();
    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error", mensaje:"Contraseña inválida."};
    const asesorName = ASESOR_CREDENTIALS[pwd];
    const num = _toNumber(data?.numeroBoleta);
    if (num===null) return {status:"error", mensaje:"Número de boleta inválido."};
    const valor = Number(data?.valorAbono)||0;
    if (valor <= 0) return {status:"error", mensaje:"El valor del abono debe ser mayor que 0."};
    const metodo = String(data?.metodoPago||"").trim();
    const refRaw = String(data?.referencia||"").trim();
    const refNorm = _normRef(refRaw);
    const esEfectivo = (refNorm==="efectivo") || (metodo.toLowerCase()==="efectivo");
    const infoVenta = _getVentaData(num);
    const telefonoCliente = infoVenta.found ? String(infoVenta.telefono||"").trim() : "";

    if (!esEfectivo){
      const tInfo = _findTransferenciaByReferencia(refRaw);
      if (tInfo.found) {
        const statusLower = String(tInfo.status||"").toLowerCase();
        if (statusLower.startsWith("asignado")) {
           if (!telefonoCliente || !statusLower.includes(telefonoCliente)) {
              return {status:"error", mensaje:`La referencia "${refRaw}" ya está ASIGNADA a otro cliente/proceso.`};
           }
        }
      }
    }

    const sheetA = _pickAbonoShardForWrite();
    if(!sheetA) return {status:"error", mensaje:"Todas las hojas de ABONOS están al límite."};
    const now = new Date();
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(10000)) return {status:"error", mensaje:"Sistema ocupado, reintenta."};
    try{
      if (!esEfectivo){
         const tInfo = _findTransferenciaByReferencia(refRaw);
         if (tInfo.found) {
            const statusLower = String(tInfo.status||"").toLowerCase();
            if (statusLower.startsWith("asignado")) {
               if (!telefonoCliente || !statusLower.includes(telefonoCliente)) {
                  return {status:"error", mensaje:`La referencia "${refRaw}" ya está ASIGNADA a otro cliente.`};
               }
            }
         }
      }

      const abonosPrevios = _listAbonos(num);
      const abonado = abonosPrevios.reduce((s,a)=> s + (Number(a.valor)||0), 0);
      const nuevoTotal = abonado + valor;
      if (nuevoTotal > TICKET_PRICE){
        const restante = Math.max(0, TICKET_PRICE - abonado);
        return {status:"error", mensaje:`El abono excede el valor del ticket. Restante permitido: ${restante}.`};
      }

      _withRetry(()=> sheetA.appendRow([
        num, valor, now, refRaw, metodo,
        data.esPendiente ? "PENDIENTE" : "",
        asesorName
      ]));

      if (!esEfectivo && telefonoCliente){
         const tInfo = _findTransferenciaByReferencia(refRaw);
         if (tInfo.found) {
           const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
           const sheetCentral = ssCentral.getSheetByName(tInfo.sheet);
           const marca = `Asignado - APARMENT - ${telefonoCliente}`;
           try {
             _withRetry(()=> sheetCentral.getRange(tInfo.row, 7).setValue(marca));
           } catch(e) { }
         }
      }
      SpreadsheetApp.flush();
    } finally {
      lock.releaseLock();
    }
    return {status:"ok"};
  }catch(err){
    return {status:"error", mensaje:`Error en el servidor (ABONO): ${String(err && err.message ? err.message : err)}`};
  }
}

function _findVentaRow(numero){
  const n = _toNumber(numero);
  for (const sh of _getAllVentaSheets()){
    const last = sh.getLastRow(); 
    if(last < 2) continue;
    const colBoletas = sh.getRange(2, 2, last-1, 1).getValues().flat().map(_toNumber);
    const idx = colBoletas.indexOf(n);
    if(idx > -1){
      const row = idx + 2;
      const vals = sh.getRange(row, 1, 1, 14).getValues()[0];
      return {
        found: true,
        sheet: sh.getName(), 
        row: row,
        data: {
          numero: vals[1],
          nombre: vals[3],  
          apellido: vals[4],
          telefono: vals[5],
          ciudad: vals[6],
          total: Number(vals[7])||0,
          restante: Number(vals[8])||0,
          asesor: vals[9],
          fecha: vals[10],
          metodo: vals[12],
          ref: vals[13]
        }
      };
    }
  }
  return {found:false};
}

function _listAbonos(numero){
  const n=_toNumber(numero);
  const out=[];
  for(const sh of _getAllAbonoSheets()){
    const last=sh.getLastRow();
    if(last<2) continue;
    const matriz = sh.getRange(2,1,last-1,7).getValues();
    for(let i=0;i<matriz.length;i++){
      const r=i+2;
      if(_toNumber(matriz[i][0])===n){
        out.push({
          sheet: sh.getName(),
          row: r,
          numero: n,
          valor: Number(matriz[i][1])||0,
          fechaHora: matriz[i][2],
          referencia: matriz[i][3]||"",
          metodo: matriz[i][4]||"",
          nota: matriz[i][5]||"",
          asesor: matriz[i][6]||""
        });
      }
    }
  }
  return out;
}

function _marcarDisponibleEnHojaNumeros(numero){
  const ss=_getSS();
  const sh=ss.getSheetByName("NUMEROS");
  if(!sh) return;
  const last=sh.getLastRow(); if(last<2) return;
  const colA = sh.getRange(2,1,last-1,1).getValues().flat().map(_pad4);
  const idx = colA.indexOf(_pad4(numero));
  if(idx>-1){
    const r=idx+2;
    sh.getRange(r,2).setValue("DISPONIBLE");
    sh.getRange(r,1).setBackground("#ffffff");
  }
}

function consultarClienteYAbonos(arg){
  try{
    const numero = (typeof arg === 'object') ? _toNumber(arg?.numero) : _toNumber(arg);
    if(numero===null) return {status:"ok", abonos:[], lista:[]};
    const venta=_findVentaRow(numero);
    const abonos=_listAbonos(numero).map(a=>{
      let fecha="", hora="";
      const v=a.fechaHora;
      if (v instanceof Date && !isNaN(v)){
        fecha = Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
        hora  = Utilities.formatDate(v, Session.getScriptTimeZone(), "hh:mm a");
      }else{
        fecha = _fechaDispToISO(v) || "";
        hora  = "";
      }
      return { sheet:a.sheet, row:a.row, numero:a.numero, valor:a.valor, referencia:a.referencia, metodo:a.metodo, fecha, hora };
    });
    return {
      status:"ok",
      venta: venta.found ? venta.data : null,
      abonos,
      lista: abonos
    };
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}

function eliminarAbonoPorFila(payload){
  try{
    let pwd="", sheetName="", row=0, numero=null;
    if (typeof payload !== 'object'){ return {status:"error",mensaje:"Parámetros inválidos."}; }
    else{
      pwd = String(payload?.contrasena||"").trim();
      sheetName = String(payload?.sheet||"").trim();
      row = Number(payload?.row||0);
      numero = _toNumber(payload?.numero);
      if(!sheetName || row<2) return {status:"error",mensaje:"Parámetros inválidos."};
    }

    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    const asesorName = ASESOR_CREDENTIALS[pwd];
    const sh=_getSheet(sheetName);
    const last=sh.getLastRow();
    if(row>last) return {status:"error",mensaje:"La fila no existe."};

    if(numero!=null){
      const n = _toNumber(sh.getRange(row,1).getValue());
      if(n!==numero) return {status:"error",mensaje:"La fila no corresponde a ese número."};
    }

    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};
    
    try{
      const datosBorrados = sh.getRange(row, 1, 1, 7).getValues()[0];
      const valorAbono = datosBorrados[1]; 
      const refAbono = datosBorrados[3];

      _withRetry(()=> sh.deleteRow(row));
      if (refAbono && _normRef(refAbono) !== "efectivo") {
         const tInfo = _findTransferenciaByReferencia(refAbono);
         if (tInfo.found) {
            const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
            const sheetCentral = ssCentral.getSheetByName(tInfo.sheet);
            _withRetry(()=> sheetCentral.getRange(tInfo.row, 7).setValue(""));
         }
      }
      
      _registrarAuditoria("ELIMINAR ABONO", numero || datosBorrados[0], asesorName, `Valor eliminado: $${valorAbono} | Ref: ${refAbono} | Hoja: ${sheetName}`);

    } finally {
      lock.releaseLock();
    }
    return {status:"ok"};
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}

function liberarNumeroYBorrarVentaYAbonos(a,b){
  try{
    let numero=null, pwd="";
    if (typeof a === 'object'){
      numero=_toNumber(a?.numero);
      pwd=String(a?.contrasena||"").trim();
    }else{
      numero=_toNumber(a);
      pwd=String(b||"").trim();
    }
    
    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    const asesorName = ASESOR_CREDENTIALS[pwd];
    if(numero===null) return {status:"error",mensaje:"Número inválido."};

    let ventasBorradas=0, abonosBorrados=0;
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};

    try{
      let telefonoCliente = null;
      let nsCliente = null; 
      const todasLasHojasVentas = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
      const ss = _getSS();

      for(const name of todasLasHojasVentas){
        const sh = ss.getSheetByName(name);
        if(!sh) continue; 
        const last=sh.getLastRow(); if(last<2) continue;
        const datosHoja = sh.getRange(2, 2, last-1, 5).getValues();
        for(let i=datosHoja.length-1; i>=0; i--){
          const numEnFila = _toNumber(datosHoja[i][0]);
          if(numEnFila === numero){
            nsCliente = String(datosHoja[i][1] || "").trim();
            telefonoCliente = String(datosHoja[i][4] || "").trim();
            _withRetry(()=> sh.deleteRow(i+2));
            ventasBorradas++;
          }
        }
      }

      for(const sh of _getAllAbonoSheets()){
        const last=sh.getLastRow();
        if(last<2) continue;
        const colA=sh.getRange(2,1,last-1,1).getValues().flat().map(_toNumber);
        for(let i=colA.length-1;i>=0;i--){
          if(colA[i]===numero){
            _withRetry(()=> sh.deleteRow(i+2));
            abonosBorrados++;
          }
        }
      }
      
      _marcarDisponibleEnHojaNumeros(numero);
      if (nsCliente || telefonoCliente) {
         SpreadsheetApp.flush();
         _reorganizarClientePostLiberacion(telefonoCliente, nsCliente);
      }

      if (ventasBorradas > 0 || abonosBorrados > 0) {
        _registrarAuditoria("LIBERAR NUMERO", numero, asesorName, `Se borró la venta y ${abonosBorrados} abonos. Cliente reorganizado.`);
      }
    } finally {
      lock.releaseLock();
    }
    return {status:"ok", ventasBorradas, abonosBorrados};
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}

function buscarTransferenciaPorReferenciaExacta(ref) {
  try {
    const needle = _normAlnum(ref);
    if (!needle) return { status: 'ok', lista: [] };
    const sheets = _getAllTransferSheets();
    if (sheets.length === 0) throw new Error('No hay hojas de TRANSFERENCIAS.');
    const out = [];
    for (const sh of sheets){
      const last = sh.getLastRow();
      if (last < 2) continue;
      const vals = sh.getRange(2, 1, last - 1, 7).getDisplayValues();
      for (let i = 0; i < vals.length; i++) {
        const row = i + 2;
        const plataforma = vals[i][1];
        const montoStr   = vals[i][2];
        const referencia = vals[i][3];
        const fechaDisp  = vals[i][4];
        const horaDisp   = vals[i][5];
        const status     = vals[i][6];
        if (_normAlnum(referencia) === needle) {
          out.push({
            sheet: sh.getName(), row, referencia,
            plataforma: plataforma || '',
            monto: Number(String(montoStr || '').replace(/[^\d]/g, '')) || 0,
            fecha: fechaDisp || '', hora: horaDisp || '', status: status || ''
          });
        }
      }
    }
    return { status: 'ok', lista: out };
  } catch (e) {
    return { status: 'error', mensaje: String(e) };
  }
}

function asignarTransferenciaPorFila(payload){
  try{
    const row=Number(payload?.row)||0;
    if(row<2) return {status:"error",mensaje:"Fila inválida."};
    const sheetName = String(payload?.sheet||"").trim();
    let sh;
    if (sheetName.toUpperCase().startsWith("TRANSFERENCIAS")) {
       const ssExterna = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
       sh = ssExterna.getSheetByName(sheetName);
    } else {
       sh = _getSheet(sheetName);
    }
    if(!sh) return {status:"error",mensaje:"No se encontró la hoja de transferencias en la Central."};
    const cur=String(sh.getRange(row,7).getValue()||"").trim().toLowerCase();
    if(cur==="asignado") return {status:"ok",mensaje:"Ya estaba asignado."};
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};
    try{
      const marca = "Asignado - APARMENT";
      _withRetry(()=> sh.getRange(row,7).setValue(marca));
    } finally {
      lock.releaseLock();
    }
    return {status:"ok"};
  }catch(err){
    return {status:"error",mensaje:`Error en el servidor (TRANSFER): ${String(err.message||err)}`};
  }
}

function buscarTransferenciasExactas(payload){
  try{
    const refNeedle = _normAlnum(payload?.referencia || "");
    const fechaISO  = String(payload?.fechaISO || "").trim();
    const hora12    = _normHora12(payload?.hora12 || "");
    const sheets = _getAllTransferSheets();
    if (sheets.length === 0) throw new Error('No hay hojas de TRANSFERENCIAS.');
    const out  = [];
    for (const sh of sheets){
      const last = sh.getLastRow();
      if (last < 2) continue;
      const vals = sh.getRange(2,1,last-1,7).getDisplayValues();
      for (let i=0; i<vals.length; i++){
        const row = i + 2;
        const plataforma = vals[i][1];
        const montoStr   = vals[i][2];
        const referencia = vals[i][3];
        const fechaDisp  = vals[i][4];
        const horaDisp   = vals[i][5];
        const status     = vals[i][6];
        if (refNeedle) {
          if (_normAlnum(referencia) === refNeedle){
            out.push({
              sheet: sh.getName(), row, plataforma: plataforma || "No identificado",
              monto: Number(String(montoStr||"").replace(/[^\d]/g,"")) || 0,
              referencia, fecha: fechaDisp, hora: horaDisp, status: status || ""
            });
          }
          continue;
        }
        const iso = _fechaDispToISO(fechaDisp);
        const h12 = _normHora12(horaDisp);
        if (iso === fechaISO && h12 === hora12){
          out.push({
            sheet: sh.getName(), row, plataforma: plataforma || "No identificado",
            monto: Number(String(montoStr||"").replace(/[^\d]/g,"")) || 0,
            referencia, fecha: fechaDisp, hora: horaDisp, status: status || ""
          });
        }
      }
    }
    return { status:"ok", lista: out };
  }catch(err){
    return { status:"error", mensaje: String(err) };
  }
}

function doGet(e){
  const page = String(e.parameter.page||"venta").toLowerCase();
  if (page==="abono"){
    return HtmlService.createHtmlOutputFromFile("FormAbono").setTitle("Registrar Abono");
  }
  if (page==="central"){
    return HtmlService.createHtmlOutputFromFile("FormCentral").setTitle("Central de Números");
  }
  return HtmlService.createHtmlOutputFromFile("FormVenta").setTitle("Registrar Venta");
}

function _registrarAuditoria(accion, numero, asesor, detalle){
  try {
    const ss = _getSS();
    const nombreHoja = "LOG_SEGURIDAD";
    let hojaLog = ss.getSheetByName(nombreHoja);
    if (!hojaLog) {
      hojaLog = ss.insertSheet(nombreHoja);
      hojaLog.appendRow(["FECHA", "HORA", "ACCIÓN", "BOLETA", "ASESOR", "DETALLES"]);
      hojaLog.getRange("A1:F1").setFontWeight("bold").setBackground("#cfe2f3");
      hojaLog.setFrozenRows(1);
    }
    const ahora = new Date();
    hojaLog.appendRow([
      ahora,
      Utilities.formatDate(ahora, Session.getScriptTimeZone(), "HH:mm:ss"),
      accion, numero, asesor, detalle
    ]);
  } catch (e) {
    console.error("Error guardando log de seguridad: " + e.message);
  }
}

function verificarCredencialesAsesor(pwd) {
  const password = String(pwd || "").trim();
  if (password in ASESOR_CREDENTIALS) {
    return { valido: true, nombre: ASESOR_CREDENTIALS[password] };
  }
  return { valido: false };
}

function _inyectarFormulas(sheet, row, valorRespaldo=0){
  if (row > 1) {
    let formulaSuma = "";
    if (_abonosUsesSingleSheet()) {
      formulaSuma = `SUMIF('${ABONOS_SINGLE_NAME}'!C1; RC[-6]; '${ABONOS_SINGLE_NAME}'!C2)`;
    } else {
      const partes = ABONOS_SHARDS.map(shName => `SUMIF('${shName}'!C1; RC[-6]; '${shName}'!C2)`);
      formulaSuma = partes.join(" + ");
    }
    const formulaTotal = `=${formulaSuma}`;
    const formulaRestante = `=${TICKET_PRICE} - RC[-1]`;
    sheet.getRange(row, 8).setFormulaR1C1(formulaTotal);
    sheet.getRange(row, 9).setFormulaR1C1(formulaRestante);
  }
}

function _getVentaData(num){
  const n = _toNumber(num);
  if (n === null) return { found:false, row:-1, total:0, restante:TICKET_PRICE, sheetName:"" };
  const todasLasHojas = _getAllBoletaSheets();
  for (const sheet of todasLasHojas){
    const last = sheet.getLastRow();
    if (last < 2) continue;
    const data = sheet.getRange(2, 1, last - 1, 14).getValues();
    for (let i=0; i<data.length; i++){
      if (_toNumber(data[i][1]) === n){
        return {
          found: true,
          row: i + 2,
          total: Number(data[i][7]) || 0,
          restante: Number(data[i][8]) || 0,
          sheetName: sheet.getName(),
          nombre: data[i][3],
          apellido: data[i][4],
          telefono: data[i][5],
          ciudad: data[i][6]
        };
      }
    }
  }
  return { found:false, row:-1, total:0, restante:TICKET_PRICE, sheetName:"" };
}

function actualizarDatosCliente(data) {
  try {
    const pwd = String(data.contrasena || "").trim();
    if (!(pwd in ASESOR_CREDENTIALS)) return { status: "error", mensaje: "Contraseña inválida." };
    const asesorName = ASESOR_CREDENTIALS[pwd];
    const numBusqueda = _toNumber(data.numero);
    if (numBusqueda === null) return { status: "error", mensaje: "Número inválido." };
    const todasLasHojas = _getAllBoletaSheets();
    let encontrado = false;
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(10000)) return { status: "error", mensaje: "Sistema ocupado." };
    try {
      for (const sheet of todasLasHojas) {
        const last = sheet.getLastRow();
        if (last < 2) continue;
        const numeros = sheet.getRange(2, 2, last - 1, 1).getValues().flat().map(_toNumber);
        const idx = numeros.indexOf(numBusqueda);
        if (idx > -1) {
          const row = idx + 2;
          sheet.getRange(row, 4).setValue(String(data.nombre || "").trim());
          sheet.getRange(row, 5).setValue(String(data.apellido || "").trim());
          sheet.getRange(row, 7).setValue(String(data.ciudad || "").trim());
          _registrarAuditoria("MODIFICAR DATOS", numBusqueda, asesorName, `Datos actualizados.`);
          encontrado = true;
          break;
        }
      }
    } finally {
      lock.releaseLock();
    }
    if (encontrado) {
      return { status: "ok", mensaje: "Datos actualizados correctamente." };
    } else {
      return { status: "error", mensaje: "No se encontró la boleta para actualizar." };
    }
  } catch (e) {
    return { status: "error", mensaje: String(e) };
  }
}

function conciliarPendientes() {
  const ss = _getSS();
  const sheetsAbonos = _getAllAbonoSheets();
  let conciliados = 0;
  console.log("Iniciando conciliación...");
  for (const sh of sheetsAbonos) {
    const last = sh.getLastRow();
    if (last < 2) continue;
    const range = sh.getRange(2, 1, last - 1, 6);
    const data = range.getValues();
    for (let i = 0; i < data.length; i++) {
      const estado = String(data[i][5] || "").trim().toUpperCase();
      const referencia = String(data[i][3] || "").trim();
      const numBoleta = data[i][0];
      if (estado === "PENDIENTE" && referencia.length > 3) {
        const tInfo = _findTransferenciaByReferencia(referencia);
        if (tInfo.found) {
          const infoVenta = _getVentaData(numBoleta);
          const telefono = infoVenta.found ? infoVenta.telefono : "SinTel";
          try {
            const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
            const shCentral = ssCentral.getSheetByName(tInfo.sheet);
            const marca = `Asignado - APARMENT - ${telefono} (Conciliado)`;
            shCentral.getRange(tInfo.row, 7).setValue(marca);
            sh.getRange(i + 2, 6).setValue("");
            conciliados++;
          } catch (e) {
            console.error(`Error conciliando ${referencia}: ${e.message}`);
          }
        }
      }
    }
  }
  return `Conciliados: ${conciliados}`;
}

function _capitalizar(texto) {
  if (!texto) return "";
  return String(texto).trim().toLowerCase().split(" ").map(palabra => {
    return palabra.charAt(0).toUpperCase() + palabra.slice(1);
  }).join(" ");
}

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    if (data.action === "registrar_desde_web") {
      const payload = {
        numeroBoleta: data.numero,
        nombre: data.nombre,
        apellido: data.apellido || "",
        telefono: data.telefono,
        ciudad: data.ciudad,
        metodoPago: data.metodoPago || "Wompi",
        primerAbono: data.monto,
        referenciaAbono: data.referencia,
        referencia: "Venta Web Automática",
        contrasena: "web_secure_key",
        esPendiente: false
      };
      if (data.esManual) {
        payload.metodoPago = "Manual/Web";
        payload.esPendiente = true;
        payload.referenciaAbono = "ESPERANDO COMPROBANTE";
      }
      const resultado = validarVentaYRegistrar(payload);
      return ContentService.createTextOutput(JSON.stringify(resultado)).setMimeType(ContentService.MimeType.JSON);
    }
    return ContentService.createTextOutput(JSON.stringify({status:"error", mensaje:"Acción desconocida"})).setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({status:"error", mensaje:"Error en Main: " + err.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}

function _reorganizarClientePostLiberacion(telefono, ns) {
  try {
    const ss = _getSS();
    const hojas = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
    const targetTel = _digits(telefono);
    const targetNS  = String(ns || "").trim();
    if (!targetTel && targetNS.length < 3) return;

    let misBoletas = [];
    let filasBorrar = [];

    hojas.forEach(nombre => {
      const sh = ss.getSheetByName(nombre);
      if (!sh) return;
      const last = sh.getLastRow();
      if (last < 2) return;
      const datos = sh.getRange(2, 1, last - 1, 14).getValues();
      for (let i = 0; i < datos.length; i++) {
        const fila = datos[i];
        const filaTel = _digits(fila[5]); 
        const matchTel = (targetTel.length > 6 && filaTel === targetTel);
        if (matchTel) {
          misBoletas.push(fila);
          filasBorrar.push({ sheet: sh, row: i + 2 });
        }
      }
    });

    if (misBoletas.length === 0) return;

    filasBorrar.sort((a, b) => {
       if (a.sheet.getName() !== b.sheet.getName()) return 0;
       return b.row - a.row;
    });
    filasBorrar.forEach(item => {
       try { item.sheet.deleteRow(item.row); } catch(e){}
    });

    misBoletas.sort((a, b) => Number(a[1]) - Number(b[1]));

    const escribirSeguro = (hoja, datosFila) => {
        const datosSinA = datosFila.slice(1); 
        const sigFila = hoja.getLastRow() + 1;
        hoja.getRange(sigFila, 2, 1, datosSinA.length).setValues([datosSinA]);
        copiarFormatoUltimaFila(hoja.getName());
        _inyectarFormulas(hoja, sigFila);
    };

    if (misBoletas.length === 1) {
       let hojaDestino = _pickVentaShardForWrite();
       if (!hojaDestino) hojaDestino = ss.getSheetByName(VARIAS_SHARDS[0]);
       if (hojaDestino) {
          escribirSeguro(hojaDestino, misBoletas[0]);
          console.log(`✅ Cliente reorganizado a individual en: ${hojaDestino.getName()}`);
       }
    } else {
       misBoletas.forEach((datos, index) => {
         const indiceHoja = index % VARIAS_SHARDS.length;
         const nombreDestino = VARIAS_SHARDS[indiceHoja];
         const shDestino = ss.getSheetByName(nombreDestino);
         if (shDestino) {
           escribirSeguro(shDestino, datos);
         }
       });
       console.log(`✅ Cliente multi reordenado (${misBoletas.length} boletas).`);
    }
  } catch (e) {
    console.error("Error en reorganización post-liberación: " + e.message);
  }
}