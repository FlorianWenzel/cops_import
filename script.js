const mysql2 = require('mysql2');
const env = require('./env');
const path = require('path');
const fs = require('fs').promises
const excel = require('xlsx');
const columns = ['Nr', 'Phase', 'Type', 'Modul', 'Aufgabe', 'Los', 'Verantwortlich', 'Firma', 'Firma', 'Unterstuetzung', 'Unterstuetzung_Firma', 'Abhaengig', 'Info_an', 'Dauer', 'Start', 'Ende', 'Real_Start', 'Real_Ende', 'Kommentar', 'App', 'Attribute', 'attribute_key']

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
    if(argument === '--dir'){
      next = true
    }
  }
  if(!next || !folder){
    console.log('please provide a absolute directory path using node script.js --dir /Your/path')
  }
  let count = 0;
  let errors = 0;
  const files = await fs.readdir(folder)
  console.log('starting import...')
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
        const { query, params } = await buildInsert('tmp_cops', data)
        await new Promise((resolve) => {
          connection.query(query, params, (err, msg) => {
            if(err)errors++
            resolve();
          })
        })
      }
      row++
    }
  }
  console.log('...done (' + (count - errors) + ' successful and ' + errors + ' errors)')
  process.exit(0)
}

main();

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
