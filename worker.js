const  { expose } = require("threads/worker");
const mysql2 = require('mysql2')
const env = require('./env.js')
const similar = require('string-similarity')

expose({
  async crunch() {
    const connection = mysql2.createConnection({
      host: env.DB_HOST,
      user: env.DB_USER,
      port: env.DB_PORT,
      multipleStatements: true,
      password: env.DB_PASS,
    })
    connection.query('USE cutovertool;')

    const all_possible_matches = await new Promise(resolve => {
/*      connection.query("select *, Step.name AS Aufgabe, Phase.name AS Phase FROM Step LEFT JOIN Phase ON phase_id = Phase.id WHERE is_blueprint = true", (err, res) => {
        resolve(res)
      })*/
      connection.query("select * from tmp_cops LIMIT 100", (err, res) => {
        resolve(res)
      })
    })
    const new_steps = await new Promise(resolve => {
      connection.query("select * from tmp_cops", (err, res) => {
        resolve(res)
      })
    })

    const matches = [];
    new_steps.forEach((new_step) => {
      if(!new_step.Aufgabe || !new_step.Phase) return;
      const steps_in_similar_phase = all_possible_matches.filter(step => {
        if(!step.Aufgabe || !step.Phase) return false;
        return similar.compareTwoStrings(step.Phase, new_step.Phase) > .9
      })
      if(steps_in_similar_phase.length < 1) return;
      const m = similar.findBestMatch(new_step.Aufgabe, steps_in_similar_phase.map(step => step.Aufgabe));
      if(m.bestMatch.rating > .8){
        matches.push({new_step, ...m})
      }
    })
    return matches;
  }
})
