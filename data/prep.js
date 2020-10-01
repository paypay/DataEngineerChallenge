const fs = require('fs')
const readline = require('readline')

const [,, input, output] = process.argv
if (!input || !output) {
  console.error('Insufficient args')
  process.exit(1)
}

const reader = readline.createInterface({
  input: fs.createReadStream(input),
  console: false
});

const ofs = fs.createWriteStream(output)

reader.on('line', (line) => {
  const s = line.split(' ')
  const ts = new Date(s[0]).getTime()
  const ip = s[2].split(':')[0]
  const url = s[12]
  ofs.write(`${ts},${ip},${url}\n`)
});

reader.on('close', () => {
  ofs.end()
})
