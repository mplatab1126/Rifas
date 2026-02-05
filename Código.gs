// ------------------------------------------------------
// Archivo: Code.gs  (backend unificado - LA PERLA NEGRA)
// - Sharding VENTAS 1/2/3 (lectura y escritura con cupo 3333)
// - ABONOS: hoja única "ABONOS" o shards "ABONOS 1/2/3" (auto-detección)
// - TRANSFERENCIAS CENTRALIZADAS: Consulta un Sheet externo (Banco Central)
// - Central de números (estado, listar/eliminar abonos, liberar número)
// - Búsqueda por boleta/teléfono, validación duplicados, lock/retry
// - URL de boleta digital por BASE_URL
// - SISTEMA DE AUDITORÍA: Registra eliminaciones y liberaciones en "LOG_SEGURIDAD"
// - CONTROL DE INVENTARIO: Solo permite vender números que existan en la hoja "NUMEROS"
// ------------------------------------------------------

// ========== CONFIG GENERAL ==========
const SPREADSHEET_ID      = "1bdJ6G7l2Iaqp1e3sp01rEYU5hbpOxWrBlm2br8DRUvE"; // ID de LA PERLA NEGRA
// ID DEL ARCHIVO "CENTRAL DE TRANSFERENCIAS" (Donde Chatea Pro guarda los pagos)
const ID_CENTRAL_TRANSFERENCIAS = "1DtwLYhRE_3PN8Sl-5We6Qr9BBF54elBhGMQoGYwG28U"; 

// CAMBIA ESTA LÍNEA AL PRINCIPIO DE TU CÓDIGO:
const BASE_URL = "https://script.google.com/macros/s/AKfycbxzRHo_XcLE-FRWQOSmq2wiM1c4WAYgEBf2vGFhYrSXtpcM7jinaoO_BxtlJpan48P5EQ/exec";
const TICKET_PRICE        = 150000;
const MAX_ROWS_PER_SHARD  = 3333;

// VENTAS shards (Nombres actualizados a V1, V2)
const VENTAS_SHARDS       = ["V1", "V2"];

// === NUEVO: CONFIGURACIÓN DE VARIAS BOLETAS (VB1 hasta VB10) ===
// Esto cubre clientes con hasta 10 boletas. Si tienen la #11, el sistema volverá a VB1 automáticamente.
const VARIAS_SHARDS       = [
  "VB1", "VB2", "VB3", "VB4", "VB5", 
  "VB6", "VB7", "VB8", "VB9", "VB10"
];
// ==============================================
// ==============================================

// ABONOS: si existe "ABONOS" se usa esa hoja; si no, shards:
const ABONOS_SHARDS       = ["ABONOS 1","ABONOS 2","ABONOS 3"];
const ABONOS_SINGLE_NAME  = "ABONOS";

// (Nombre “base” solo informativo; la lógica usa _getAllTransferSheets)
const TRANSFERENCIAS_NAME = "TRANSFERENCIAS";

// Asesores
const ASESOR_CREDENTIALS = {
  "m8a3": "Mateo","r0j5": "Manu R","s14": "Saldarriaga","a2n7": "Anyeli","a9e1": "Alejo",
  "m26": "Nena","l22": "Luisa","s19": "Lili","v261": "Vale","l20": "Arias","a21": "Aleja",
  "of": "Oficina",
  "j1" : "Jennifer","mo2":"Andres","ca1":"Carlos",
  "web_secure_key": "Página Web" // <--- NUEVO ASESOR VIRTUAL
};

// ========== UTILIDADES ==========
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
  const m=s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/); if(!m) return "";
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

// === FUNCIÓN RENOMBRADA Y BLINDADA ===
function _verificarInventarioFINAL(n){
  // Convertimos a formato "0000" (Texto) para comparar peras con peras
  const buscado = _pad4(n);
  if (buscado === "0000" && n != 0) return false; // Validación básica

  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ss.getSheetByName("NUMEROS");
  if (!sh) return false;

  const last = sh.getLastRow();
  if (last < 2) return false;

  // Leemos la Columna A tal cual la ven tus ojos (Texto)
  const inventario = sh.getRange(2, 1, last - 1, 1).getDisplayValues().flat();

  // Búsqueda exacta de texto
  for (let i = 0; i < inventario.length; i++) {
    // _pad4 asegura que "7771" sea igual a "7771" y "0922" a "922"
    if (_pad4(inventario[i]) === buscado) {
      return true; // ¡ENCONTRADO!
    }
  }

  return false;
}
// ===============================================

// === NUEVA FUNCIÓN AUXILIAR ===
// Obtiene todas las hojas donde puede haber boletas (Ventas + Varias)
function _getAllBoletaSheets() {
  // Asegúrate de que VENTAS_SHARDS y VARIAS_SHARDS estén definidos arriba
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
  return ssExterna.getSheets().filter(s => /^TRANSFERENCIAS.*$/i.test(s.getName()));
}

// ========== SHARDS: VENTAS ==========
function _getAllVentaSheets(){ return VENTAS_SHARDS.map(n=>_getSheet(n)); }
function _pickVentaShardForWrite(){
  for(const name of VENTAS_SHARDS){
    const sh=_getSheet(name);
    const dataRows=Math.max(0, sh.getLastRow()-1);
    if(dataRows < MAX_ROWS_PER_SHARD) return sh;
  }
  return null;
}

// ========== SHARDS: ABONOS ==========
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

// ========== FORMATO ==========
function copiarFormatoUltimaFila(sheetName){
  const sh=_getSheet(sheetName);
  const lastRow = sh.getLastRow();
  if (lastRow <= 2) return;
  const origen  = sh.getRange(lastRow - 1, 1, 1, sh.getLastColumn());
  const destino = sh.getRange(lastRow, 1, 1, sh.getLastColumn());
  origen.copyTo(destino, SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
}

// ========== BUSCAR (OPTIMIZADO: BOLETA, TELÉFONO O NS) ==========
function handleGetRequest(data){
  const numReq = _toNumber(data.numeroBoleta);
  const telReq = _digits(data.telefono);
  const nsReq  = String(data.nsUsuario || "").trim(); // <--- NUEVO PARAMETRO
  
  const resultados = [];

  // Helper interno (se mantiene igual, optimizado para leer filas)
  function buildBoletaObjOptimizado(sheet, rowData, rowIndex){
    // rowData: [0:Org, 1:Num, 2:NS, 3:Nom, 4:Ape, 5:Tel, 6:Ciu, 7:Tot, 8:Res, 9:Ase, 10:Fec, 11:Link, 12:Met, 13:Ref]
    
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
      nsUsuario: rowData[2] // Incluimos el NS en la respuesta
    };
  }

  // 1. BÚSQUEDA POR NÚMERO DE BOLETA (Prioridad 1)
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
    // Si no está vendido, verificamos inventario
    if (_verificarInventarioFINAL(numReq)) {
      return { status: "disponible", numero: numReq };
    } else {
      return { status: "noInventario", numero: numReq };
    }
  }

  // 2. BÚSQUEDA POR TELÉFONO (Prioridad 2)
  if (telReq){
    for (const sheet of _getAllBoletaSheets()){
      const last = sheet.getLastRow();
      if (last < 2) continue;

      // Leemos Columna F (Teléfono) -> Índice 6 (en getRange)
      const colTels = sheet.getRange(2, 6, last - 1, 1).getDisplayValues().flat().map(_digits);
      
      for (let i = 0; i < colTels.length; i++) {
        if (colTels[i] === telReq){ // Coincidencia exacta de dígitos
           const rowValues = sheet.getRange(i + 2, 1, 1, 14).getValues()[0];
           resultados.push(buildBoletaObjOptimizado(sheet, rowValues, i + 2));
        }
      }
    }
  }

  // 3. BÚSQUEDA POR NS DE USUARIO (Prioridad 3 - NUEVO BLOQUE)
  if (nsReq && !telReq && numReq === null) {
    for (const sheet of _getAllBoletaSheets()){
      const last = sheet.getLastRow();
      if (last < 2) continue;

      // Leemos Columna C (NS) -> Índice 3 (en getRange)
      const colNS = sheet.getRange(2, 3, last - 1, 1).getDisplayValues().flat();
      
      for (let i = 0; i < colNS.length; i++) {
        // Comparamos texto exacto (trim)
        if (String(colNS[i]).trim() === nsReq){
           const rowValues = sheet.getRange(i + 2, 1, 1, 14).getValues()[0];
           resultados.push(buildBoletaObjOptimizado(sheet, rowValues, i + 2));
        }
      }
    }
  }

  // RETORNO DE RESULTADOS MÚLTIPLES (Teléfono o NS)
  if (resultados.length > 0) {
    if (resultados.length === 1) return { status:"encontrado", datos: resultados[0] };
    resultados.sort((a,b)=> Number(a.numero) - Number(b.numero));
    return { status:"multiples", lista: resultados };
  }

  return { status:"noExiste" };
}

// ========== VENTAS ==========
// ========== SEGURIDAD: VALIDAR DUPLICADOS GLOBALMENTE ==========
function existeBoleta(n){
  const numBuscado = _toNumber(n);
  if (numBuscado === null) return false;

  const todasLasHojas = _getAllBoletaSheets();
  for (const sheet of todasLasHojas){
    const last = sheet.getLastRow();
    if (last < 2) continue;
    
    // Leemos Columna B (Fila 2, Col 2)
    const valores = sheet.getRange(2, 2, last - 1, 1).getValues();
    
    for (let i = 0; i < valores.length; i++) {
       const numEnCelda = _toNumber(valores[i][0]);
       if (numEnCelda === numBuscado) {
         return true; 
       }
    }
  }
  return false;
}

function _referenciaAbonoExisteEnAbonos(ref){
  const refNeedle=_normRef(ref); if(!refNeedle) return false;
  const sheets=_getAllAbonoSheets();
  for (const sh of sheets){
    const last=sh.getLastRow(); if(last<2) continue;
    const refs=sh.getRange(2,4,last-1,1).getValues().flat().map(_normRef); // Col D: referencia
    if (refs.includes(refNeedle)) return true;
  }
  return false;
}

// === NUEVO: buscar transferencia (todas las hojas) y saber si está Asignada ===
function _findTransferenciaByReferencia(ref){
  const needle = _normAlnum(ref);
  if(!needle) return {found:false};

  const sheets = _getAllTransferSheets(); // Ahora busca en el CENTRAL
  if (sheets.length === 0) return {found:false};

  for (const sh of sheets){
    const last = sh.getLastRow();
    if (last < 2) continue;
    const vals = sh.getRange(2,1,last-1,7).getDisplayValues(); // A:G
    for (let i=0;i<vals.length;i++){
      const referencia = vals[i][3];
      if (_normAlnum(referencia) === needle){
        const row = i+2;
        const plataforma = vals[i][1];
        const montoStr   = vals[i][2];
        const fechaDisp  = vals[i][4];
        const horaDisp   = vals[i][5];
        const status     = String(vals[i][6]||"").trim();
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
  return String(t.status||"").toLowerCase().startsWith("asignado"); // Modificado para aceptar "Asignado - Estacas"
}

// =============================================================================
// FUNCIÓN PRINCIPAL DE REGISTRO (BLINDADA + MULTICOMPRADOR + INVENTARIO)
// =============================================================================
function validarVentaYRegistrar(data){
  try{
    const pwd=String(data.contrasena||"").trim(); 
    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    const asesorName=ASESOR_CREDENTIALS[pwd];

    // === VARIABLE CLAVE: TELEFONO CLIENTE ===
    const TELEFONO_CLIENTE = String(data.telefono||"").trim();

    // === NUEVO: CAPTURAR EL NS DE USUARIO ===
    const NS_USUARIO = String(data.nsUsuario || "").trim();
    
    // VALIDACIÓN OBLIGATORIA DEL NS
    if (NS_USUARIO === "") {
       return {status: "error", mensaje: "El campo 'NS DEL USUARIO' es obligatorio."};
    }
    // ========================================
    // ========================================

    // VALIDACIÓN: MÉTODO DE PAGO
    const metodoValidacion = String(data.metodoPago||"").trim();
    if (!metodoValidacion || metodoValidacion === "" || metodoValidacion === "Selecciona...") {
       return {status:"error", mensaje:"El campo 'Método de pago' es obligatorio."};
    }

    const num=_toNumber(data.numeroBoleta); 
    if(num===null) return {status:"error",mensaje:"Boleta inválida."};

  
    // CAMBIO: Usamos la función BLINDADA
    if (!_verificarInventarioFINAL(num)) return {status:"error", mensaje:`El número ${data.numeroBoleta} NO pertenece a tu inventario autorizado.`};
    if (existeBoleta(num)) return {status:"duplicada",mensaje:`La boleta ${data.numeroBoleta} ya fue vendida.`};

    const m0=Number(data.primerAbono)||0; 
    const refPrimerAbono=String(data.referenciaAbono||"").trim();
    const refEsEfectivo = _normRef(refPrimerAbono) === "efectivo";
    const refProvista   = !!refPrimerAbono && !refEsEfectivo;

    // === BLOQUEO INTELIGENTE POR REFERENCIA ===
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
    // Validamos espacio antes de entrar al lock para no bloquear innecesariamente
    const sheetV_Check = _pickVentaShardForWrite(); 
    if(!sheetA || !sheetV_Check) return {status:"error",mensaje:"No hay hojas disponibles (Ventas o Abonos llenas)."};

    const now = new Date();
    const lock = LockService.getScriptLock();
    
    // CANDADO FUERTE: Esperar hasta 30 seg si está ocupado
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema muy ocupado, intenta de nuevo."};

    try{
      // ------------------------------------------------------------------------
      // DOBLE VERIFICACIÓN DE SEGURIDAD (CRUCIAL CONTRA DUPLICADOS)
      // ------------------------------------------------------------------------
      // Volvemos a revisar si se vendió en el último milisegundo mientras esperábamos el lock
      if (existeBoleta(num)) {
         return {status:"duplicada", mensaje:`¡Lo siento! Alguien acaba de comprar la boleta ${data.numeroBoleta} hace un instante.`};
      }
      // ------------------------------------------------------------------------

      // ====================================================================================
      // 1. LÓGICA DE "VARIAS BOLETAS" (DETECTAR Y MOVER)
      // ====================================================================================
      let sheetV = null; // Aquí guardaremos la hoja destino final
      let ventaPreviaMover = null; 
      let hojaOrigenPrevia = null; 
      let filaOrigenPrevia = 0;

      // A) Buscar si ya existe en VENTAS (Shards normales)
      let existeEnVentas = false;
      for (const nombreHoja of VENTAS_SHARDS) {
        const sh = _getSheet(nombreHoja);
        const last = sh.getLastRow();
        if (last < 2) continue;
        // Cambiamos el 3 por el 6 (Columna F = Teléfono)
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

      // B) Buscar cuántas tiene ya en VARIAS BOLETAS (Lógica Dual: Teléfono O NS)
      let conteoVarias = 0;
      const telTarget = _digits(TELEFONO_CLIENTE); // Teléfono limpio
      const nsTarget  = String(data.nsUsuario || "").trim(); // El NS que llega del formulario
      
      for (const nombreHoja of VARIAS_SHARDS) {
        const sh = _getSS().getSheetByName(nombreHoja);
        if (!sh) continue; 
        const last = sh.getLastRow();
        if (last < 2) continue;
        
        // Leemos desde la Columna A hasta la F (6 columnas)
        // Columna C (índice 2) = NS
        // Columna F (índice 5) = Teléfono
        const datosHoja = sh.getRange(2, 1, last - 1, 6).getValues(); 
        
        // Filtramos para contar coincidencias (SOLO TELÉFONO)
        const encontradosEnHoja = datosHoja.filter(fila => {
           // Columna F (índice 5) = Teléfono
           const telFila = _digits(fila[5]); 
           
           // COINCIDENCIA POR TELÉFONO (Único criterio válido)
           const matchTel = (telTarget.length > 6 && telFila === telTarget);
           
           return matchTel;
        }).length;
        
        conteoVarias += encontradosEnHoja;
      }

      // C) DECIDIR EL DESTINO
      if (existeEnVentas) {
        // ESCENARIO: Tiene 1 en Ventas (V1/V2) -> Mover vieja a VB1, nueva a VB2
        sheetV = _getSheet(VARIAS_SHARDS[1]); 
      } else if (conteoVarias > 0) {
        // ESCENARIO: Ya es multicomprador.
        // CORRECCIÓN: Usamos Módulo (%) para distribución cíclica.
        // Ejemplo: Si tiene 2 boletas, conteoVarias=2. Indice 2 es "VB3".
        // Ejemplo: Si tiene 10 boletas, conteoVarias=10. 10 % 10 = 0. Indice 0 es "VB1".
        let indiceDestino = conteoVarias % VARIAS_SHARDS.length;
        sheetV = _getSheet(VARIAS_SHARDS[indiceDestino]);
      } else {
        // ESCENARIO: Cliente Nuevo -> Ventas normal (V1 o V2)
        sheetV = _pickVentaShardForWrite();
      }

      if(!sheetV) return {status:"error",mensaje:"No hay hojas de VENTAS/VARIAS disponibles."};

      // ====================================================================================
      // 2. EJECUCIÓN DE MOVIMIENTOS Y REGISTROS
      // ====================================================================================

      // PASO A: Si hay que mover una venta vieja, lo hacemos AHORA
      if (existeEnVentas && ventaPreviaMover) {
        // Al usar VARIAS_SHARDS[0], estamos apuntando automáticamente a "VB1"
        const shDestinoVieja = _getSheet(VARIAS_SHARDS[0]); 
        
        // --- CORRECCIÓN CLAVE: Escribir sin tocar la Columna A ---
        const nuevaFilaVieja = shDestinoVieja.getLastRow() + 1;
        
        // "ventaPreviaMover" trae los datos viejos. 
        // Usamos .slice(1) para quitar el primer dato (Columna A) y quedarnos solo con B en adelante.
        const datosLimpios = ventaPreviaMover.slice(1);
        
        // Escribimos desde la Columna 2 (B) hacia la derecha
        _withRetry(()=> shDestinoVieja.getRange(nuevaFilaVieja, 2, 1, datosLimpios.length).setValues([datosLimpios]));
        // ---------------------------------------------------------
        
        // Borramos la venta vieja de VENTAS X (V1 o V2)
        _withRetry(()=> hojaOrigenPrevia.deleteRow(filaOrigenPrevia));

        // Regeneramos fórmulas y formato visual
        copiarFormatoUltimaFila(shDestinoVieja.getName()); 
        _inyectarFormulas(shDestinoVieja, nuevaFilaVieja, ventaPreviaMover[4] || 0); 
      }

      // PASO B: Registrar el Abono de la VENTA NUEVA
      if(m0 > 0){
      // DEFINIMOS LA NOTA: Si es pendiente ponemos el texto, si no, vacío.
      const estadoNota = data.esPendiente ? "PENDIENTE" : "";

      _withRetry(()=> sheetA.appendRow([
        num, m0, now, refPrimerAbono, data.metodoPago||"", estadoNota, asesorName
      ]));

        // Marcar transferencia (con teléfono)
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

      // PASO C: Registrar la VENTA NUEVA (ESCRIBIENDO DESDE COLUMNA B)
      // Definimos los datos SIN la comilla vacía inicial, porque ya no vamos a tocar la Col A
      const datosVentaNueva = [
        num,                                     // B: NUMERO BOLETA
        NS_USUARIO,                              // C: NS DEL USUARIO
        _capitalizar(data.nombre),               // D: NOMBRE
        _capitalizar(data.apellido),             // E: APELLIDO
        TELEFONO_CLIENTE,                        // F: TELEFONO
        _capitalizar(data.ciudad),               // G: CIUDAD
        0,                                       // H: TOTAL ABONADO
        0,                                       // I: RESTANTE
        asesorName,                              // J: ASESOR
        now,                                     // K: FECHA
        "",                                      // L: BOLETA DIGITAL
        String(data.metodoPago||"").trim(),      // M: METODO DE PAGO
        String(data.referencia||"").trim()       // N: REFERENCIA ANUNCIO
      ];

      // Calculamos la siguiente fila libre
      const newRow = sheetV.getLastRow() + 1;
      
      // Escribimos desde la Columna 2 (B) hasta la 14 (N) -> Total 13 columnas
      // IMPORTANTE: Esto evita tocar la Columna A y previene el error #REF!
      _withRetry(()=> sheetV.getRange(newRow, 2, 1, 13).setValues([datosVentaNueva]));

      // Generar URL y guardarla en la Columna L (Columna 12)
      const padded=("0000"+num).slice(-4);
      const url=`${BASE_URL}?numero=${padded}`;
      _withRetry(()=> sheetV.getRange(newRow, 12).setValue(url)); // <--- CAMBIO: Columna 12 (L)

      copiarFormatoUltimaFila(sheetV.getName());
      
      // PASO D: Inyectar fórmulas a la VENTA NUEVA
      _inyectarFormulas(sheetV, newRow);

      // PASO E: MARCAR EN LA HOJA DE INVENTARIO "NUMEROS"
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

// === NUEVA FUNCIÓN AUXILIAR: MARCAR VENDIDO ===
// Pégala al final de tu archivo Code.gs
function _marcarVendidoEnInventario(numero){
  try {
    const ss = _getSS();
    const sh = ss.getSheetByName("NUMEROS");
    if (!sh) return;

    const last = sh.getLastRow(); 
    if (last < 2) return;

    // Buscamos el número en la columna A
    // Convertimos a número para asegurar coincidencia
    const listaNumeros = sh.getRange(2, 1, last - 1, 1).getValues().flat().map(_toNumber);
    const target = _toNumber(numero);
    
    const idx = listaNumeros.indexOf(target);
    
    if (idx > -1) {
      // La fila es idx + 2 (por el encabezado y el índice 0)
      // Columna B (2) es el ESTADO
      const celdaEstado = sh.getRange(idx + 2, 2);
      celdaEstado.setValue("VENDIDO");
      // Opcional: Poner el fondo rojo o el estilo que uses para vendidos
      // sh.getRange(idx + 2, 1).setBackground("#ea9999"); 
    }
  } catch(e) {
    console.error("Error marcando inventario: " + e.message);
  }
}

/** Compatibilidad: acepta payload {numero} o directamente "4190"/4190 */
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

// --- NUEVO: Registrar ABONO (OPTIMIZADO PARA PAGOS MASIVOS) ---
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

    // 1. BUSCAMOS EL DUEÑO DE LA BOLETA (Para obtener el teléfono y validar pertenencia)
    const infoVenta = _getVentaData(num);
    const telefonoCliente = infoVenta.found ? String(infoVenta.telefono||"").trim() : "";

    // Chequeos preliminares (fuera del lock)
    if (!esEfectivo){
      // Lógica de Bloqueo Inteligente
      const tInfo = _findTransferenciaByReferencia(refRaw);
      
      if (tInfo.found) {
        const statusLower = String(tInfo.status||"").toLowerCase();
        // Si ya está marcada como ASIGNADO...
        if (statusLower.startsWith("asignado")) {
           // Verificamos si la marca contiene EL MISMO teléfono del cliente actual.
           // Si NO lo contiene (o no encontramos teléfono), bloqueamos.
           if (!telefonoCliente || !statusLower.includes(telefonoCliente)) {
              return {status:"error", mensaje:`La referencia "${refRaw}" ya está ASIGNADA a otro cliente/proceso.`};
           }
        }
      } else {
        // *** IMPORTANTE: RELAJACIÓN DE SEGURIDAD LOCAL ***
        // Hemos comentado esta validación para permitir que el MISMO cliente use la referencia en varias boletas (Multicomprador).
        // Si la reactivas, el segundo abono fallará.
        
        /*
        if (_referenciaAbonoExisteEnAbonos(refRaw)){
           return {status:"error", mensaje:`La referencia "${refRaw}" ya fue registrada en un abono previo.`};
        }
        */
      }
    }

    const sheetA = _pickAbonoShardForWrite();
    if(!sheetA) return {status:"error", mensaje:"Todas las hojas de ABONOS están al límite. Crea otra hoja o aumenta el cupo."};

    const now = new Date();
    const lock = LockService.getScriptLock();
    // Reducimos tiempo de espera a 10s para agilizar el ciclo masivo
    if (!lock.tryLock(10000)) return {status:"error", mensaje:"Sistema ocupado, reintenta."};

    try{
      // Revalidaciones dentro del lock (evita condiciones de carrera)
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
         // Nota: Aquí tampoco validamos _referenciaAbonoExisteEnAbonos para permitir el lote.
      }

      const abonosPrevios = _listAbonos(num);
      const abonado = abonosPrevios.reduce((s,a)=> s + (Number(a.valor)||0), 0);
      const nuevoTotal = abonado + valor;
      if (nuevoTotal > TICKET_PRICE){
        const restante = Math.max(0, TICKET_PRICE - abonado);
        return {status:"error", mensaje:`El abono excede el valor del ticket. Restante permitido: ${restante}.`};
      }

      _withRetry(()=> sheetA.appendRow([
        num,                 // A: número
        valor,               // B: valor
        now,                 // C: fecha/hora
        refRaw,              // D: referencia
        metodo,              // E: método
        data.esPendiente ? "PENDIENTE" : "", // F: nota
        asesorName           // G: asesor
      ]));

      // 3. MARCAR TRANSFERENCIA EN LA CENTRAL (CON TELÉFONO)
      // Esto asegura que si es el primer abono con esta ref, quede firmada con el teléfono del cliente.
      if (!esEfectivo && telefonoCliente){
         const tInfo = _findTransferenciaByReferencia(refRaw);
         if (tInfo.found) {
           const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
           const sheetCentral = ssCentral.getSheetByName(tInfo.sheet);
           
           const marca = `Asignado - APARMENT - ${telefonoCliente}`;
           
           // Usamos try-catch silencioso por si otro proceso la está escribiendo
           try {
             _withRetry(()=> sheetCentral.getRange(tInfo.row, 7).setValue(marca));
           } catch(e) { /* Ignorar error de escritura concurrente */ }
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

// CORREGIDO: Busca en Columna B (Índice 2 para getRange, Índice 1 para Array)
function _findVentaRow(numero){
  const n = _toNumber(numero);
  
  for (const sh of _getAllVentaSheets()){
    const last = sh.getLastRow(); 
    if(last < 2) continue;
    
    // CAMBIO CRÍTICO: Leemos la Columna B (2), no la A (1)
    const colBoletas = sh.getRange(2, 2, last-1, 1).getValues().flat().map(_toNumber);
    const idx = colBoletas.indexOf(n);
    
    if(idx > -1){
      const row = idx + 2;
      // Leemos toda la fila para sacar los datos (A..N) -> 14 columnas
      const vals = sh.getRange(row, 1, 1, 14).getValues()[0]; 
      
      // Mapeo correcto según tu nueva estructura:
      // A[0]Org, B[1]Num, C[2]NS, D[3]Nom, E[4]Ape, F[5]Tel, G[6]Ciu, H[7]Tot, I[8]Res, J[9]Ase, K[10]Fec, L[11]Link, M[12]Met, N[13]Ref
      return {
        found: true,
        sheet: sh.getName(), 
        row: row,
        data: {
          numero: vals[1],    // Col B
          nombre: vals[3],    // Col D
          apellido: vals[4],  // Col E
          telefono: vals[5],  // Col F
          ciudad: vals[6],    // Col G
          total: Number(vals[7])||0,    // Col H
          restante: Number(vals[8])||0, // Col I
          asesor: vals[9],    // Col J
          fecha: vals[10],    // Col K
          metodo: vals[12],   // Col M
          ref: vals[13]       // Col N
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
    const last=sh.getLastRow(); if(last<2) continue;
    const matriz = sh.getRange(2,1,last-1,7).getValues(); // A..G
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

// ========== CENTRAL: ENDPOINTS CON AUDITORÍA ==========
function central_estadoNumero(payload){
  try{
    const numero = (typeof payload === 'object') ? _toNumber(payload?.numero) : _toNumber(payload);
    if(numero===null) return {status:"error",mensaje:"Número inválido."};

    const venta=_findVentaRow(numero);
    const abonos=_listAbonos(numero);
    const abonado = abonos.reduce((s,a)=>s+(Number(a.valor)||0),0);
    const restante = Math.max(0, TICKET_PRICE - abonado);
    const estado = venta.found ? "VENDIDO" : (abonado>0 ? "EN PROCESO" : "DISPONIBLE");

    return {
      status:"ok",
      numero:_pad4(numero),
      estado,
      cliente: venta.found ? {
        nombre: venta.data.nombre || "",
        apellido: venta.data.apellido || "",
        telefono: venta.data.telefono || "",
        ciudad: venta.data.ciudad || ""
      } : null,
      totalAbonado: abonado,
      restante,
      abonos
    };
  }catch(e){
    return {status:"error",mensaje:String(e)};
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
        hora  = Utilities.formatDate(v, Session.getScriptTimeZone(), "hh:mm a");
      }else{
        fecha = _fechaDispToISO(v) || "";
        hora  = "";
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
    let pwd="", sheetName="", row=0, numero=null, legacy=false;

    if (typeof payload !== 'object'){
      legacy = true;
      row = Number(payload);
      if (isNaN(row) || row<2) return {status:"error",mensaje:"Fila inválida."};
      if (_abonosUsesSingleSheet()){
        sheetName = ABONOS_SINGLE_NAME;
      }else{
        return {status:"error",mensaje:"Falta 'sheet'. Con shards de ABONOS debes enviar {sheet,row}."};
      }
    }else{
      pwd = String(payload?.contrasena||"").trim();
      sheetName = String(payload?.sheet||"").trim();
      row = Number(payload?.row||0);
      numero = _toNumber(payload?.numero);
      if(!sheetName || row<2) return {status:"error",mensaje:"Parámetros inválidos."};
    }

    // Validación de seguridad
    if (!legacy){
      if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    }
    const asesorName = legacy ? "Desconocido (Legacy)" : ASESOR_CREDENTIALS[pwd];

    const sh=_getSheet(sheetName);
    const last=sh.getLastRow(); if(row>last) return {status:"error",mensaje:"La fila no existe."};

    // Verificar número
    if(numero!=null){
      const n = _toNumber(sh.getRange(row,1).getValue());
      if(n!==numero) return {status:"error",mensaje:"La fila no corresponde a ese número."};
    }

    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};
    
    try{
      // --- CAPTURA DE EVIDENCIA ANTES DE BORRAR ---
      const datosBorrados = sh.getRange(row, 1, 1, 7).getValues()[0]; 
      const valorAbono = datosBorrados[1]; // Columna B es Valor
      const refAbono = datosBorrados[3];   // Columna D es Referencia
      // --------------------------------------------

      _withRetry(()=> sh.deleteRow(row));

      // === NUEVO: LIBERAR TRANSFERENCIA EN LA CENTRAL ===
      // Si el abono tenía referencia (y no es efectivo), la buscamos y le borramos el "Asignado..."
      if (refAbono && _normRef(refAbono) !== "efectivo") {
         const tInfo = _findTransferenciaByReferencia(refAbono);
         
         if (tInfo.found) {
            const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
            const sheetCentral = ssCentral.getSheetByName(tInfo.sheet);
            
            // Borramos el contenido de la columna G (7) => Status
            _withRetry(()=> sheetCentral.getRange(tInfo.row, 7).setValue(""));
         }
      }
      // ==================================================
      
      // --- DISPARO DE CÁMARA DE SEGURIDAD ---
      _registrarAuditoria(
        "ELIMINAR ABONO", 
        numero || datosBorrados[0], 
        asesorName, 
        `Valor eliminado: $${valorAbono} | Ref: ${refAbono} | Hoja: ${sheetName}`
      );
      // ---------------------------------------

    } finally {
      lock.releaseLock();
    }
    return {status:"ok"};
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}

function borrarAbonoPorFila(row){
  if (_abonosUsesSingleSheet()){
    return eliminarAbonoPorFila({sheet: ABONOS_SINGLE_NAME, row: Number(row)||0});
  }
  return {status:"error", mensaje:"No se pudo eliminar sin 'sheet'. Actualiza el formulario para enviar {contrasena, sheet, row}."};
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
    
    // Validación estricta
    if(!(pwd in ASESOR_CREDENTIALS)) return {status:"error",mensaje:"Contraseña inválida."};
    const asesorName = ASESOR_CREDENTIALS[pwd];
    
    if(numero===null) return {status:"error",mensaje:"Número inválido."};

    let ventasBorradas=0, abonosBorrados=0;
    // Variable para capturar el teléfono antes de borrar
    let telefonoCliente = null;

    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};

    try{
      // Variables para capturar datos del cliente ANTES de borrarlo
      let telefonoCliente = null;
      let nsCliente = null; // NUEVO

      // 1. BORRAR DE VENTAS (BUSCANDO EN TODAS LAS HOJAS)
      const todasLasHojasVentas = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
      const ss = _getSS();

      for(const name of todasLasHojasVentas){
        const sh = ss.getSheetByName(name);
        if(!sh) continue; 

        const last=sh.getLastRow(); if(last<2) continue;
        
        // Leemos hasta la columna F (Teléfono) para capturar datos
        // B(Num)=0, C(NS)=1, D, E, F(Tel)=4  (Índices relativos del getRange)
        // getRange(fila, col, numFilas, numCols). Leemos desde Col 2 (B) hasta Col 6 (F) -> 5 columnas
        const datosHoja = sh.getRange(2, 2, last-1, 5).getValues();
        
        for(let i=datosHoja.length-1; i>=0; i--){
          // datosHoja[i][0] es Columna B (Número)
          const numEnFila = _toNumber(datosHoja[i][0]);
          
          if(numEnFila === numero){
            // CAPTURAMOS LOS DATOS PARA LA REORGANIZACIÓN
            nsCliente = String(datosHoja[i][1] || "").trim(); // Columna C (índice relativo 1) - NUEVO
            telefonoCliente = String(datosHoja[i][4] || "").trim(); // Columna F (índice relativo 4)
            
            _withRetry(()=> sh.deleteRow(i+2));
            ventasBorradas++;
          }
        }
      }

      // 2. BORRAR DE ABONOS
      for(const sh of _getAllAbonoSheets()){
        const last=sh.getLastRow(); if(last<2) continue;
        const colA=sh.getRange(2,1,last-1,1).getValues().flat().map(_toNumber);
        for(let i=colA.length-1;i>=0;i--){
          if(colA[i]===numero){
            _withRetry(()=> sh.deleteRow(i+2));
            abonosBorrados++;
          }
        }
      }
      
      // 3. MARCAR COMO DISPONIBLE EN EL INVENTARIO
      _marcarDisponibleEnHojaNumeros(numero);
      
      // 4. REORGANIZAR CLIENTE (LÓGICA NUEVA Y AUTOMÁTICA)
      // Si capturamos el NS o el Teléfono, ejecutamos la mudanza de lo que sobró
      if (nsCliente || telefonoCliente) {
         SpreadsheetApp.flush(); // Guardar cambios antes de reorganizar
         // Llamamos a la nueva función que ordena 1->Varias1, 2->Varias2
         _reorganizarClientePostLiberacion(telefonoCliente, nsCliente);
      }

      // --- DISPARO DE CÁMARA DE SEGURIDAD ---
      if (ventasBorradas > 0 || abonosBorrados > 0) {
        _registrarAuditoria(
          "LIBERAR NUMERO", 
          numero, 
          asesorName, 
          `Se borró la venta y ${abonosBorrados} abonos. Cliente reorganizado.`
        );
      }

    } finally {
      lock.releaseLock();
    }
    return {status:"ok", ventasBorradas, abonosBorrados};
  }catch(e){
    return {status:"error",mensaje:String(e)};
  }
}

// ========== TRANSFERENCIAS (BUSCAR / ASIGNAR) ==========

/** Busca coincidencia EXACTA por referencia en TODAS las hojas que empiecen con "TRANSFERENCIAS". */
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
      const vals = sh.getRange(2, 1, last - 1, 7).getDisplayValues(); // A:G
      for (let i = 0; i < vals.length; i++) {
        const row = i + 2;
        const plataforma = vals[i][1]; // B
        const montoStr   = vals[i][2]; // C
        const referencia = vals[i][3]; // D
        const fechaDisp  = vals[i][4]; // E
        const horaDisp   = vals[i][5]; // F
        const status     = vals[i][6]; // G

        if (_normAlnum(referencia) === needle) {
          out.push({
            sheet: sh.getName(),
            row,
            referencia,
            plataforma: plataforma || '',
            monto: Number(String(montoStr || '').replace(/[^\d]/g, '')) || 0,
            fecha: fechaDisp || '',
            hora: horaDisp || '',
            status: status || ''
          });
        }
      }
    }
    return { status: 'ok', lista: out };
  } catch (e) {
    return { status: 'error', mensaje: String(e) };
  }
}

/** Marca como Asignado por fila. ACEPTA {sheet, row}. Si no envían sheet, usa la primera hoja que exista. */
function asignarTransferenciaPorFila(payload){
  try{
    const row=Number(payload?.row)||0; if(row<2) return {status:"error",mensaje:"Fila inválida."};
    const sheetName = String(payload?.sheet||"").trim();
    
    let sh;
    
    // LÓGICA NUEVA: Si la hoja se llama "TRANSFERENCIAS...", la buscamos en el archivo externo
    if (sheetName.toUpperCase().startsWith("TRANSFERENCIAS")) {
       const ssExterna = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
       sh = ssExterna.getSheetByName(sheetName);
    } else {
       // Si es otra cosa (raro), la buscamos en el archivo local
       sh = _getSheet(sheetName);
    }

    if(!sh) return {status:"error",mensaje:"No se encontró la hoja de transferencias en la Central."};

    const cur=String(sh.getRange(row,7).getValue()||"").trim().toLowerCase();
    if(cur==="asignado") return {status:"ok",mensaje:"Ya estaba asignado."};

    const lock = LockService.getScriptLock();
    if (!lock.tryLock(30000)) return {status:"error", mensaje:"Sistema ocupado, inténtalo de nuevo."};

    try{
      // IMPORTANTE: Agregamos de qué rifa se asignó para que sepas cuál se lo llevó
      // En este caso como es LA PERLA NEGRA, pondremos "Asignado - LA PERLA NEGRA"
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

/** También permite búsqueda por fechaISO+hora12 exactas, revisando todas las hojas de TRANSFERENCIAS. */
function buscarTransferenciasExactas(payload){
  try{
    const refNeedle = _normAlnum(payload?.referencia || "");
    const fechaISO  = String(payload?.fechaISO || "").trim();
    const hora12    = _normHora12(payload?.hora12 || "");

    const sheets = _getAllTransferSheets();
    if (sheets.length === 0) throw new Error('No hay hojas de TRANSFERENCIAS.');

    const out  = [];
    for (const sh of sheets){
      const last = sh.getLastRow();
      if (last < 2) continue;
      const vals = sh.getRange(2,1,last-1,7).getDisplayValues(); // A:G

      for (let i=0; i<vals.length; i++){
        const row        = i + 2;
        const plataforma = vals[i][1];
        const montoStr   = vals[i][2];
        const referencia = vals[i][3];
        const fechaDisp  = vals[i][4];
        const horaDisp   = vals[i][5];
        const status     = vals[i][6];

        if (refNeedle) {
          if (_normAlnum(referencia) === refNeedle){
            out.push({
              sheet: sh.getName(),
              row,
              plataforma: plataforma || "No identificado",
              monto: Number(String(montoStr||"").replace(/[^\d]/g,"")) || 0,
              referencia,
              fecha: fechaDisp,
              hora:  horaDisp,
              status: status || ""
            });
          }
          continue;
        }

        const iso = _fechaDispToISO(fechaDisp);
        const h12 = _normHora12(horaDisp);
        if (iso === fechaISO && h12 === hora12){
          out.push({
            sheet: sh.getName(),
            row,
            plataforma: plataforma || "No identificado",
            monto: Number(String(montoStr||"").replace(/[^\d]/g,"")) || 0,
            referencia,
            fecha: fechaDisp,
            hora:  horaDisp,
            status: status || ""
          });
        }
      }
    }
    return { status:"ok", lista: out };
  }catch(err){
    return { status:"error", mensaje: String(err) };
  }
}

// ========== OTRAS UTILIDADES ==========
function backfillBoletaDigital(){
  try{
    for (const name of VENTAS_SHARDS){
      const sheet=_getSheet(name);
      const last=sheet.getLastRow(); if(last<2) continue;
      const nums=sheet.getRange(2,1,last-1,1).getValues().flat();
      const existing=sheet.getRange(2,7,last-1,1).getValues().flat();

      const updated=nums.map((n,i)=>{
        if(existing[i]&&String(existing[i]).trim()!=="") return [existing[i]];
        if(!n) return [""];
        const padded=("0000"+n).slice(-4);
        const url=`${BASE_URL}?numero=${padded}`;
        return [url];
      });

      const lock = LockService.getScriptLock();
      if (!lock.tryLock(30000)) throw new Error("Sistema ocupado");
      try{
        _withRetry(()=> sheet.getRange(2,7,updated.length,1).setValues(updated));
      } finally {
        lock.releaseLock();
      }
    }
  }catch(err){
    // opcional
  }
}

// ========== ROUTING HTML ==========
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

// ========== SISTEMA DE AUDITORÍA (LOG SEGURIDAD) ==========
function _registrarAuditoria(accion, numero, asesor, detalle){
  try {
    const ss = _getSS();
    const nombreHoja = "LOG_SEGURIDAD";
    let hojaLog = ss.getSheetByName(nombreHoja);
    
    // Si la hoja no existe, la crea automáticamente y pone los títulos
    if (!hojaLog) {
      hojaLog = ss.insertSheet(nombreHoja);
      hojaLog.appendRow(["FECHA", "HORA", "ACCIÓN", "BOLETA", "ASESOR", "DETALLES"]);
      hojaLog.getRange("A1:F1").setFontWeight("bold").setBackground("#cfe2f3");
      hojaLog.setFrozenRows(1);
    }
    
    const ahora = new Date();
    // Escribe el renglón de seguridad
    hojaLog.appendRow([
      ahora,                                     // Fecha completa
      Utilities.formatDate(ahora, Session.getScriptTimeZone(), "HH:mm:ss"), // Hora
      accion,                                    // Qué hizo (Eliminar/Liberar)
      numero,                                    // Boleta afectada
      asesor,                                    // Quién fue
      detalle                                    // Info extra (ej. valor borrado)
    ]);
    
  } catch (e) {
    // Si falla el log, no detenemos el proceso principal, pero avisamos en consola
    console.error("Error guardando log de seguridad: " + e.message);
  }
}

// ========== FUNCION DE LOGIN ==========
function verificarCredencialesAsesor(pwd) {
  const password = String(pwd || "").trim();
  if (password in ASESOR_CREDENTIALS) {
    return { 
      valido: true, 
      nombre: ASESOR_CREDENTIALS[password] 
    };
  }
  return { valido: false };
}

// === HELPER INTERNO PARA FÓRMULAS (AJUSTADO A NUEVO ORDEN) ===
function _inyectarFormulas(sheet, row, valorRespaldo=0){
  if (row > 1) {
    let formulaSuma = "";
    
    // NOTA: La columna de TOTAL es la H (8). El NÚMERO está en la B (2).
    // La distancia de H a B es -6 columnas (RC[-6]).
    
    if (_abonosUsesSingleSheet()) {
      // Fórmula con punto y coma (;) para región Colombia/Latam
      formulaSuma = `SUMIF('${ABONOS_SINGLE_NAME}'!C1; RC[-6]; '${ABONOS_SINGLE_NAME}'!C2)`;
    } else {
      const partes = ABONOS_SHARDS.map(shName => `SUMIF('${shName}'!C1; RC[-6]; '${shName}'!C2)`);
      formulaSuma = partes.join(" + ");
    }

    const formulaTotal = `=${formulaSuma}`;
    const formulaRestante = `=${TICKET_PRICE} - RC[-1]`; // Restante es I, Total es H (una atrás)

    // Columna H (8) es TOTAL, Columna I (9) es RESTANTE
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
    // Leemos todo el rango hasta la columna L (12) o N (14)
    const data = sheet.getRange(2, 1, last - 1, 14).getValues();
    
    for (let i=0; i<data.length; i++){
      // Comparamos Columna B (índice 1)
      if (_toNumber(data[i][1]) === n){
        return {
          found: true,
          row: i + 2,
          total: Number(data[i][7]) || 0,    // Col H (7)
          restante: Number(data[i][8]) || 0, // Col I (8)
          sheetName: sheet.getName(),
          nombre: data[i][3],                // Col D (3)
          apellido: data[i][4],              // Col E (4)
          telefono: data[i][5],              // Col F (5)
          ciudad: data[i][6]                 // Col G (6)
        };
      }
    }
  }
  return { found:false, row:-1, total:0, restante:TICKET_PRICE, sheetName:"" };
}

// === HELPER: REORGANIZAR CLIENTE TRAS LIBERACIÓN ===
// Si al cliente le queda solo 1 boleta y está en "VARIAS", la mueve a "VENTAS"
function _reorganizarClienteSiQuedaSolo(telefono){
  try {
    const targetTel = _digits(telefono);
    if(!targetTel) return;

    const ss = _getSS();
    const todasHojas = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
    
    let boletasEncontradas = [];

    // 1. Buscar todas las boletas que le quedan a este cliente en todo el sistema
    for (const name of todasHojas) {
      const sh = ss.getSheetByName(name);
      if(!sh) continue;
      const last = sh.getLastRow();
      if (last < 2) continue;
      
      // Leemos toda la hoja para tener los datos completos para mover
      const data = sh.getRange(2, 1, last - 1, 12).getValues(); 
      
      for (let i = 0; i < data.length; i++) {
        // Columna F (índice 5) es el teléfono
            if (_digits(data[i][5]) === targetTel) {
           boletasEncontradas.push({
             sheetName: name,
             rowIndex: i + 2, // Fila real en la hoja (base 1)
             dataRow: data[i] // Datos completos de la fila
           });
        }
      }
    }

    // 2. Evaluar si necesita mudanza
    // Condición: Tiene EXACTAMENTE 1 boleta restante Y esa boleta NO está en VENTAS (está en Varias)
    if (boletasEncontradas.length === 1) {
       const boleta = boletasEncontradas[0];
       const estaEnVentas = VENTAS_SHARDS.includes(boleta.sheetName);

       if (!estaEnVentas) {
          // ¡DETECTADO! Está solo en "Varias Boletas". Hay que moverlo a "Ventas".
          const hojaOrigen = ss.getSheetByName(boleta.sheetName);
          const hojaDestino = _pickVentaShardForWrite(); // Busca espacio en Ventas 1 o 2

          if (hojaOrigen && hojaDestino) {
             // A) Copiar a destino (Ventas)
             _withRetry(()=> hojaDestino.appendRow(boleta.dataRow));
             
             // B) Borrar de origen (Varias)
             _withRetry(()=> hojaOrigen.deleteRow(boleta.rowIndex));
             
             // C) Regenerar fórmulas en la nueva fila de destino
             const nuevaFila = hojaDestino.getLastRow();
             copiarFormatoUltimaFila(hojaDestino.getName()); // Asegúrate de tener esta función o usar copiarFormatoUltimaFila
             _inyectarFormulas(hojaDestino, nuevaFila);
          }
       }
    }

  } catch (e) {
    console.error("Error reorganizando cliente: " + e.message);
  }
}

// === NUEVO: ACTUALIZAR DATOS DEL CLIENTE (CENTRAL) ===
function actualizarDatosCliente(data) {
  try {
    const pwd = String(data.contrasena || "").trim();
    if (!(pwd in ASESOR_CREDENTIALS)) return { status: "error", mensaje: "Contraseña inválida." };
    const asesorName = ASESOR_CREDENTIALS[pwd];

    const numBusqueda = _toNumber(data.numero);
    if (numBusqueda === null) return { status: "error", mensaje: "Número inválido." };

    // Buscar en todas las hojas (Ventas y Varias)
    const todasLasHojas = _getAllBoletaSheets();
    let encontrado = false;

    const lock = LockService.getScriptLock();
    if (!lock.tryLock(10000)) return { status: "error", mensaje: "Sistema ocupado." };

    try {
      for (const sheet of todasLasHojas) {
        const last = sheet.getLastRow();
        if (last < 2) continue;

        // ... (dentro del loop de búsqueda)
        // Leemos columna B (2) para buscar el número
        const numeros = sheet.getRange(2, 2, last - 1, 1).getValues().flat().map(_toNumber);
        const idx = numeros.indexOf(numBusqueda);

        if (idx > -1) {
          const row = idx + 2;
          
          // Actualizamos:
          // Col D (4): Nombre
          sheet.getRange(row, 4).setValue(String(data.nombre || "").trim());
          // Col E (5): Apellido
          sheet.getRange(row, 5).setValue(String(data.apellido || "").trim());
          // Col G (7): Ciudad
          sheet.getRange(row, 7).setValue(String(data.ciudad || "").trim());
          
          // ... log de seguridad ...

          // LOG DE SEGURIDAD
          _registrarAuditoria(
            "MODIFICAR DATOS",
            numBusqueda,
            asesorName,
            `Antes: ${viejos[0]} ${apellidoViejo} (${viejos[2]}) - Ahora: ${data.nombre} ${data.apellido} (${data.ciudad})`
          );

          encontrado = true;
          break; // Terminamos
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

// === ROBOT CONCILIADOR: Revisa pendientes y los cruza con la Central ===
function conciliarPendientes() {
  const ss = _getSS();
  const sheetsAbonos = _getAllAbonoSheets();
  let conciliados = 0;

  console.log("Iniciando conciliación...");

  for (const sh of sheetsAbonos) {
    const last = sh.getLastRow();
    if (last < 2) continue;

    // Leemos abonos: A(Num), B(Valor), C(Fecha), D(Ref), E(Met), F(Nota)
    const range = sh.getRange(2, 1, last - 1, 6);
    const data = range.getValues();
    
    for (let i = 0; i < data.length; i++) {
      const estado = String(data[i][5] || "").trim().toUpperCase(); // Col F
      const referencia = String(data[i][3] || "").trim();          // Col D
      const numBoleta = data[i][0];

      // Si dice PENDIENTE, intentamos buscarlo en la central
      if (estado === "PENDIENTE" && referencia.length > 3) {
        
        const tInfo = _findTransferenciaByReferencia(referencia);
        
        if (tInfo.found) {
          // ¡APARECIÓ EN LA CENTRAL!
          
          // 1. Buscamos teléfono para firmar
          const infoVenta = _getVentaData(numBoleta);
          const telefono = infoVenta.found ? infoVenta.telefono : "SinTel";

          try {
            // 2. Marcamos en Central
            const ssCentral = SpreadsheetApp.openById(ID_CENTRAL_TRANSFERENCIAS);
            const shCentral = ssCentral.getSheetByName(tInfo.sheet);
            const marca = `Asignado - APARMENT - ${telefono} (Conciliado)`;
            shCentral.getRange(tInfo.row, 7).setValue(marca);

            // 3. Quitamos "PENDIENTE" en hoja de Abonos
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

// --- FUNCIÓN PARA CORREGIR NOMBRES (Mayúsculas/Minúsculas) ---
function _capitalizar(texto) {
  if (!texto) return "";
  // 1. Convertir todo a minúscula
  // 2. Separar por espacios
  // 3. Poner mayúscula la primera letra de cada palabra
  return String(texto).trim().toLowerCase().split(" ").map(palabra => {
    return palabra.charAt(0).toUpperCase() + palabra.slice(1);
  }).join(" ");
}


// ======================================================
// PUERTA DE ENLACE PARA LA PÁGINA WEB (API RECEIVER)
// ======================================================
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    
    // Si la acción es registrar venta externa
    if (data.action === "registrar_desde_web") {
      
      // Armamos el paquete tal como lo espera tu función validarVentaYRegistrar
      const payload = {
        numeroBoleta: data.numero,
        nombre: data.nombre,
        apellido: data.apellido || "", // <--- CAMBIO 1: Aquí recibimos el apellido separado
        telefono: data.telefono, // Ya debe venir con 57 si la web lo pone
        ciudad: data.ciudad,
        metodoPago: data.metodoPago || "Wompi", // <--- CAMBIO 2: Aceptamos el método que envía la API (para ver si es RECHAZADA)
        primerAbono: data.monto,
        referenciaAbono: data.referencia, // ID de transacción Wompi
        referencia: "Venta Web Automática", // Fuente del anuncio
        contrasena: "web_secure_key", // La clave que creamos en el paso A
        esPendiente: false // Wompi ya está pagado, no es pendiente (o true si es reserva manual)
      };

      // Si es reserva manual (sin pago confirmado aún)
      if (data.esManual) {
        payload.metodoPago = "Manual/Web";
        payload.esPendiente = true; // Para que use la lógica de pendiente
        payload.referenciaAbono = "ESPERANDO COMPROBANTE";
      }

      // ¡AQUÍ OCURRE LA MAGIA! Usamos tu lógica principal
      const resultado = validarVentaYRegistrar(payload);
      
      return ContentService.createTextOutput(JSON.stringify(resultado))
        .setMimeType(ContentService.MimeType.JSON);
    }
    
    return ContentService.createTextOutput(JSON.stringify({status:"error", mensaje:"Acción desconocida"}))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({status:"error", mensaje:"Error en Main: " + err.toString()}))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// === VERSIÓN CORREGIDA: NO TOCA LA COLUMNA A ===
function _reorganizarClientePostLiberacion(telefono, ns) {
  try {
    const ss = _getSS();
    const hojas = [...VENTAS_SHARDS, ...VARIAS_SHARDS];
    
    // Preparar datos de búsqueda
    const targetTel = _digits(telefono);
    const targetNS  = String(ns || "").trim();
    
    if (!targetTel && targetNS.length < 3) return;

    let misBoletas = [];
    let filasBorrar = [];

    // 1. RECOLECTAR LO QUE LE QUEDA AL CLIENTE
    hojas.forEach(nombre => {
      const sh = ss.getSheetByName(nombre);
      if (!sh) return;
      const last = sh.getLastRow();
      if (last < 2) return;

      // Leemos todo (A-N)
      const datos = sh.getRange(2, 1, last - 1, 14).getValues();

      for (let i = 0; i < datos.length; i++) {
        const fila = datos[i];
        // Solo extraemos el teléfono (Columna F, índice 5)
        const filaTel = _digits(fila[5]); 

        // Verificamos coincidencia (SOLO TELÉFONO)
        const matchTel = (targetTel.length > 6 && filaTel === targetTel);

        if (matchTel) {
          misBoletas.push(fila);
          filasBorrar.push({ sheet: sh, row: i + 2 });
        }
      }
    });

    if (misBoletas.length === 0) return;

    // 2. BORRAR TODO LO VIEJO
    filasBorrar.sort((a, b) => {
       if (a.sheet.getName() !== b.sheet.getName()) return 0;
       return b.row - a.row; // De abajo hacia arriba
    });
    filasBorrar.forEach(item => {
       try { item.sheet.deleteRow(item.row); } catch(e){}
    });

    // 3. DECIDIR DÓNDE ESCRIBIR (CORREGIDO)
    misBoletas.sort((a, b) => Number(a[1]) - Number(b[1])); // Ordenar por número

    // Helper para escribir SIN tocar Columna A
    const escribirSeguro = (hoja, datosFila) => {
        // datosFila tiene indices 0..13 (A..N). 
        // Queremos escribir desde B..N (indices 1..13).
        // .slice(1) corta el primer elemento (Col A)
        const datosSinA = datosFila.slice(1); 
        
        const sigFila = hoja.getLastRow() + 1;
        // Escribimos desde fila nueva, Columna 2 (B), 1 fila de alto, N columnas de ancho
        hoja.getRange(sigFila, 2, 1, datosSinA.length).setValues([datosSinA]);
        
        // Regenerar fórmulas y formato
        copiarFormatoUltimaFila(hoja.getName());
        _inyectarFormulas(hoja, sigFila);
    };

    // === CASO A: LE QUEDA SOLO 1 BOLETA -> MOVER A "VENTAS" ===
    if (misBoletas.length === 1) {
       let hojaDestino = _pickVentaShardForWrite();
       if (!hojaDestino) hojaDestino = ss.getSheetByName(VARIAS_SHARDS[0]);
       
       if (hojaDestino) {
          escribirSeguro(hojaDestino, misBoletas[0]);
          console.log(`✅ Cliente reorganizado a individual en: ${hojaDestino.getName()}`);
       }
    }
    
    // === CASO B: LE QUEDAN VARIAS -> DISTRIBUIR EN "VARIAS" ===
    else {
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