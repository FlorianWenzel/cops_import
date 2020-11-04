const { spawn, Thread, Worker } = require('threads')
const mysql2 = require('mysql2');
const env = require('./env');
const path = require('path');
const fs = require('fs').promises
const excel = require('xlsx');
const columns = ['Nr', 'Phase', 'Type', 'Aufgabe', 'Los', 'Verantwortlich', 'Firma', 'Firma', 'Unterstuetzung', 'Unterstuetzung_Firma', 'Abhaengig', 'Info_an', 'Dauer', 'Start', 'Ende', 'Real_Start', 'Real_Ende', 'Kommentar', 'App', 'Attribute', 'attribute_key']
let skip_import = false;
const ora = require('ora');

async function main(){

  const connection = mysql2.createConnection({
    host: env.DB_HOST,
    user: env.DB_USER,
    port: env.DB_PORT,
    multipleStatements: true,
    password: env.DB_PASS,
  })
  connection.query('USE cutovertool;')
  let folder = '';
  let next = false;
  for(const argument of process.argv){
    if(next){
      folder = argument
      break;
    }
    if(argument === '--clear'){
      const spinnerClear = ora("Clearing data")
      connection.query(`
          DROP TABLE IF EXISTS tmp_cops;
          CREATE TABLE \`tmp_cops\` (
        \`Filename\` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
        \`Nr\` int NOT NULL,
        \`Phase\` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Type\` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Modul\` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Aufgabe\` varchar(3000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Los\` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Verantwortlich\` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Firma\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Unterstuetzung\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Unterstuetzung_Firma\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Abhaengig\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Info_an\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Dauer\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Start\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Ende\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Real_Start\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Real_Ende\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Kommentar\` varchar(4000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`App\` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`Attribute\` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
        \`attribute_key\` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
      `)
      spinnerClear.succeed("Cleared data")
    }
    if(argument === '--skip-import'){
      skip_import = true;
    }
    if(argument === '--dir'){
      next = true
    }
  }

  if(!next || !folder){
    console.log('please provide a absolute directory path using node script.js --dir /Your/path')
  }

  if(!skip_import){
    const spinnerImport = ora('Importing Files').start();
    let count = 0;
    let errors = 0;
    const files = await fs.readdir(folder)
    for(const file of files){
      const workbook = excel.readFile(path.join(folder, file));
      const worksheet = workbook.Sheets[workbook.SheetNames[0]]
      let rows_left = true;
      let row = 9;
      while(rows_left){
        const data = {}
        data.Filename = file;

        columns.forEach((column, i) => {
          const letter = String.fromCharCode(i + 65)
          data[column] = worksheet[letter+row] ? worksheet[letter+row].v : null
        })
        if(!data.Nr){
          rows_left = false;
        }else{
          count++;
          const {query, params} = await buildInsert('tmp_cops', data)
          await new Promise((resolve) => {
            connection.query(query, params, (err) => {
              if(err){
                errors++
              }
              resolve();
            })
          })
        }
        row++
      }
    }
    spinnerImport.succeed("Import complete - ("+count + " rows)");
  }


  const spinnerCrunching = ora('Crunching numbers').start();
  const auth = await spawn(new Worker("./worker.js"))
  const matches = await auth.crunch()
  await Thread.terminate(auth)
  spinnerCrunching.succeed("Numbers crunched")

  console.log('sort by module and ')
  const by_filename = {}
  for(const match of matches){
    const {new_step} = match;
    const filename = new_step.Filename.replace('.xlsx', '');
    if(!by_filename[filename]) by_filename[filename] = []
    by_filename[filename].push(match)
  }
  for(const filename of Object.keys(by_filename)){
    by_filename[filename].forEach((steps, index) => {
      const high_rated = steps.ratings.filter(({rating}) => rating > .9);
      console.log(high_rated)
    })
  }

}


main().then(r => process.exit(0));

async function buildInsert (table, body) {
  if(!body){
    console.log(body)
  }
  let query = 'INSERT INTO ' + table + ' ('

  delete body.id
  for (const field in body) {
    query += '`' + field + '`, '
  }
  query = query.slice(0, -2)
  query += ') VALUES (?'
  query += ',?'.repeat(Object.keys(body).length - 1)
  query += ')'
  const params = Object.values(body)

  return { query, params }
}
