saifelislaam/file.d
  Saif

function moveButtonRandomly() { const btnRect =

198

noBtn.getBoundingClientRect(); const maxWidth = window.innerWidth - btnRect.width;

const maxHeight = window.innerHeight - btnRect.height;

const randomX =

Math.floor(Math.random() * maxWidth);

const randomY = Math.floor(Math.random() * maxHeight);

noBtn.style.setProperty("left", "${randomX} px');

noBtn.style.setProperty("top", "${randomY}

px');

}
