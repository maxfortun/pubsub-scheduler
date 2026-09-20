import { createCanvas, loadImage } from 'canvas';
import { readFileSync, writeFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import path from 'path';

const svgPath = process.argv[2] || 'docs/articles/linkedin-splash.svg';
const outputPath = process.argv[3] || svgPath.replace('.svg', '.jpg');

const svgContent = readFileSync(svgPath, 'utf-8');

// Parse SVG to get dimensions
const dom = new JSDOM(svgContent, { contentType: 'image/svg+xml' });
const svgElement = dom.window.document.querySelector('svg');
const width = parseInt(svgElement.getAttribute('width')) || 1200;
const height = parseInt(svgElement.getAttribute('height')) || 627;

// Create canvas and draw
const canvas = createCanvas(width, height);
const ctx = canvas.getContext('2d');

// Convert SVG to data URL and load as image
const svgDataUrl = `data:image/svg+xml;base64,${Buffer.from(svgContent).toString('base64')}`;

loadImage(svgDataUrl).then((image) => {
  ctx.drawImage(image, 0, 0, width, height);
  const jpegBuffer = canvas.toBuffer('image/jpeg', { quality: 0.95 });
  writeFileSync(outputPath, jpegBuffer);
  console.log(`Created: ${outputPath} (${width}x${height})`);
}).catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
