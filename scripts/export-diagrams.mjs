import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';

const DIAGRAMS_DIR = './docs/diagrams';
const IMAGES_DIR = './docs/diagrams/images';

if (!fs.existsSync(IMAGES_DIR)) {
  fs.mkdirSync(IMAGES_DIR, { recursive: true });
}

const files = fs.readdirSync(DIAGRAMS_DIR).filter(f => f.endsWith('.excalidraw'));
console.log(`Found ${files.length} diagrams to export`);

for (const file of files) {
  const inputPath = path.join(DIAGRAMS_DIR, file);
  const baseName = file.replace('.excalidraw', '');
  const svgPath = path.join(IMAGES_DIR, `${baseName}.svg`);

  console.log(`Processing: ${file}`);

  try {
    const data = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
    const elements = data.elements || [];

    if (elements.length === 0) {
      console.log(`  Skipping: no elements`);
      continue;
    }

    const validEls = elements.filter(e => e.x !== undefined && e.y !== undefined);
    const minX = Math.min(...validEls.map(e => e.x)) - 30;
    const minY = Math.min(...validEls.map(e => e.y)) - 30;
    const maxX = Math.max(...validEls.map(e => (e.x || 0) + (e.width || 100))) + 30;
    const maxY = Math.max(...validEls.map(e => (e.y || 0) + (e.height || 50))) + 30;

    const width = maxX - minX;
    const height = maxY - minY;

    let svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${minX} ${minY} ${width} ${height}" width="${width}" height="${height}">
  <rect x="${minX}" y="${minY}" width="${width}" height="${height}" fill="white"/>
  <defs>
    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#1e1e1e"/>
    </marker>
  </defs>
  <style>
    text { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .mono { font-family: 'SF Mono', Monaco, Consolas, monospace; }
  </style>
`;

    for (const el of elements) {
      if (el.type === 'rectangle') {
        const dash = el.strokeStyle === 'dashed' ? 'stroke-dasharray="8,4"' : '';
        const rx = el.roundness ? 8 : 0;
        svg += `  <rect x="${el.x}" y="${el.y}" width="${el.width}" height="${el.height}" fill="${el.backgroundColor || 'transparent'}" stroke="${el.strokeColor || '#1e1e1e'}" stroke-width="${el.strokeWidth || 1}" rx="${rx}" ${dash}/>\n`;
      }
      else if (el.type === 'ellipse') {
        const cx = el.x + el.width / 2;
        const cy = el.y + el.height / 2;
        svg += `  <ellipse cx="${cx}" cy="${cy}" rx="${el.width / 2}" ry="${el.height / 2}" fill="${el.backgroundColor || 'transparent'}" stroke="${el.strokeColor || '#1e1e1e'}" stroke-width="${el.strokeWidth || 1}"/>\n`;
      }
      else if (el.type === 'text') {
        const fs = el.fontSize || 14;
        const lines = (el.text || '').split('\n');
        const anchor = el.textAlign === 'center' ? 'middle' : 'start';
        const xOff = el.textAlign === 'center' ? (el.width || 0) / 2 : 0;
        const cls = el.fontFamily === 3 ? 'class="mono"' : '';

        for (let i = 0; i < lines.length; i++) {
          const y = el.y + fs * 0.85 + i * (fs * 1.2);
          svg += `  <text x="${el.x + xOff}" y="${y}" font-size="${fs}" fill="${el.strokeColor || '#1e1e1e'}" text-anchor="${anchor}" ${cls}>${esc(lines[i])}</text>\n`;
        }
      }
      else if (el.type === 'line' || el.type === 'arrow') {
        const pts = el.points || [[0, 0], [100, 0]];
        const x1 = el.x + pts[0][0];
        const y1 = el.y + pts[0][1];
        const x2 = el.x + pts[pts.length - 1][0];
        const y2 = el.y + pts[pts.length - 1][1];
        const dash = el.strokeStyle === 'dashed' ? 'stroke-dasharray="8,4"' : '';
        const marker = el.type === 'arrow' ? 'marker-end="url(#arrowhead)"' : '';
        svg += `  <line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${el.strokeColor || '#1e1e1e'}" stroke-width="${el.strokeWidth || 1}" ${dash} ${marker}/>\n`;
      }
    }

    svg += `</svg>`;
    fs.writeFileSync(svgPath, svg);
    console.log(`  ✓ ${svgPath}`);

  } catch (err) {
    console.error(`  ✗ Error: ${err.message}`);
  }
}

function esc(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

console.log('\nDone! SVG files created.');
console.log('To convert to PNG, install librsvg: brew install librsvg');
console.log('Then run: for f in docs/diagrams/images/*.svg; do rsvg-convert -w 1200 "$f" > "${f%.svg}.png"; done');
