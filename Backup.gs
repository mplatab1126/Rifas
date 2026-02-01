// ------------------------------------------------------
// Archivo: Backup.gs  (guardar copias en carpeta específica)
// ------------------------------------------------------

// SOLO define la carpeta destino. NO vuelvas a declarar SPREADSHEET_ID aquí.
const BACKUP_FOLDER_ID = "1iYlyAht5ZLWUngYOihhE-yQz2QJVee76";  // <-- cambia por tu ID de carpeta

function backupToFolder() {
  // Usa la SPREADSHEET_ID que ya tienes declarada en tu proyecto
  const file   = DriveApp.getFileById(SPREADSHEET_ID);
  const folder = DriveApp.getFolderById(BACKUP_FOLDER_ID);

  // Nombre seguro (sin dos puntos) con fecha/hora local
  const ts   = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd_HH-mm");
  const name = `Backup Rifa - ${ts}`;

  file.makeCopy(name, folder);
  Logger.log("✅ Backup creado: " + name);
}

/* OPCIONAL: mantener solo los últimos N archivos de backup en la carpeta */
function pruneBackups_keepLastN() {
  const N = 60; // por ejemplo, conserva los últimos 60 backups
  const folder = DriveApp.getFolderById(BACKUP_FOLDER_ID);

  // recopila archivos cuyo nombre empieza por "Backup Rifa - "
  const files = [];
  const it = folder.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    if (f.getName().startsWith("Backup Rifa - ")) files.push(f);
  }

  // ordena del más reciente al más antiguo (por fecha de última actualización)
  files.sort((a, b) => b.getLastUpdated() - a.getLastUpdated());

  // borra los que sobren
  for (let i = N; i < files.length; i++) {
    files[i].setTrashed(true); // mándalo a la papelera
  }
  Logger.log(`🧹 Limpieza completa. Conservados: ${Math.min(N, files.length)}.`);
}