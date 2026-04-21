
const fs = require('fs');
const content = fs.readFileSync('c:\\Users\\Dell Latitude 5420\\Downloads\\APP\\elim-reporting-app\\js\\app.js', 'utf8');
const lines = content.split('\n');
const dashboardLines = lines.slice(1994, 2265); // 1995 to 2265 (0-indexed)
const text = dashboardLines.join('\n');

let braces = 0;
let backticks = 0;

for (let i = 0; i < text.length; i++) {
    if (text[i] === '{') braces++;
    if (text[i] === '}') braces--;
    if (text[i] === '`') backticks++;
}

console.log('Dashboard function audit:');
console.log('Braces balance:', braces);
console.log('Backticks count:', backticks);
console.log('First 50 chars of range:', text.substring(0, 50));
console.log('Last 50 chars of range:', text.substring(text.length - 50));
