/* clock.js*/

const canvas = document.getElementById("analogClock");
const ctx = canvas.getContext("2d");
const radius = canvas.height / 2 - 10;
ctx.translate(canvas.width / 2, canvas.height / 2);
setInterval(drawClock, 1000);

function drawClock() {
  ctx.clearRect(-canvas.width / 2, -canvas.height / 2, canvas.width, canvas.height);
  drawFace(ctx, radius);
  drawTicks(ctx, radius);
  drawNumbers(ctx, radius);
  drawTime(ctx, radius);
  drawDateInside(ctx, radius);
}

function drawFace(ctx, radius) {
  ctx.beginPath();
  ctx.arc(0, 0, radius, 0, 2 * Math.PI);
  ctx.fillStyle = "white";
  ctx.fill();

  const grad = ctx.createRadialGradient(0, 0, radius * 0.95, 0, 0, radius * 1.05);
  grad.addColorStop(0, "#333");
  grad.addColorStop(0.5, "white");
  grad.addColorStop(1, "#333");

  ctx.strokeStyle = grad;
  ctx.lineWidth = radius * 0.05;
  ctx.stroke();

  ctx.beginPath();
  ctx.arc(0, 0, radius * 0.05, 0, 2 * Math.PI);
  ctx.fillStyle = "#333";
  ctx.fill();
}

function drawTicks(ctx, radius) {
  for (let i = 0; i < 60; i++) {
    let angle = (i * Math.PI) / 30;
    let inner = i % 5 === 0 ? radius * 0.82 : radius * 0.9;
    let outer = radius * 0.97;

    ctx.beginPath();
    ctx.lineWidth = i % 5 === 0 ? 3 : 1;
    ctx.strokeStyle = "#000";
    ctx.moveTo(inner * Math.cos(angle), inner * Math.sin(angle));
    ctx.lineTo(outer * Math.cos(angle), outer * Math.sin(angle));
    ctx.stroke();
  }
}

function drawNumbers(ctx, radius) {
  ctx.font = radius * 0.15 + "px Arial";
  ctx.textBaseline = "middle";
  ctx.textAlign = "center";
  for (let num = 1; num < 13; num++) {
    let ang = num * Math.PI / 6;
    ctx.rotate(ang);
    ctx.translate(0, -radius * 0.7);
    ctx.rotate(-ang);
    ctx.fillText(num.toString(), 0, 0);
    ctx.rotate(ang);
    ctx.translate(0, radius * 0.7);
    ctx.rotate(-ang);
  }
}

function drawTime(ctx, radius) {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  const eat = new Date(utc + 3 * 3600000);

  let hour = eat.getHours();
  let minute = eat.getMinutes();
  let second = eat.getSeconds();

  hour = hour % 12;
  hour = (hour * Math.PI / 6) + (minute * Math.PI / (6 * 60)) + (second * Math.PI / (360 * 60));
  drawHand(ctx, hour, radius * 0.5, radius * 0.07);
  minute = (minute * Math.PI / 30) + (second * Math.PI / (30 * 60));
  drawHand(ctx, minute, radius * 0.75, radius * 0.07);
  second = second * Math.PI / 30;
  drawHand(ctx, second, radius * 0.9, radius * 0.02, "red");
}

function drawHand(ctx, pos, length, width, color = "black") {
  ctx.beginPath();
  ctx.lineWidth = width;
  ctx.lineCap = "round";
  ctx.strokeStyle = color;
  ctx.moveTo(0, 0);
  ctx.rotate(pos);
  ctx.lineTo(0, -length);
  ctx.stroke();
  ctx.rotate(-pos);
}

function drawDateInside(ctx, radius) {
  const now = new Date();
  const day = String(now.getDate()).padStart(2, "0");
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const year = String(now.getFullYear()).slice(-0);
  const dateString = `${day}-${month}-${year}`;

  ctx.font = radius * 0.14 + "px Arial";
  ctx.fillStyle = "#000";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(dateString, 0, radius * 0.30);
}
