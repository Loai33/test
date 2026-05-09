import { Component, viewChild, ElementRef, afterNextRender, NgZone, inject, DestroyRef } from '@angular/core';
import * as THREE from 'three';

function addRipple(e: MouseEvent, btn: HTMLButtonElement) {
  const rect = btn.getBoundingClientRect();
  const ripple = document.createElement('span');
  ripple.className = 'ripple';
  const size = Math.max(rect.width, rect.height);
  ripple.style.cssText = `width:${size}px;height:${size}px;left:${e.clientX - rect.left - size / 2}px;top:${e.clientY - rect.top - size / 2}px;`;
  btn.appendChild(ripple);
  setTimeout(() => ripple.remove(), 700);
}

function addLightFollow(e: MouseEvent, btn: HTMLButtonElement) {
  const rect = btn.getBoundingClientRect();
  const x = ((e.clientX - rect.left) / rect.width) * 100;
  const y = ((e.clientY - rect.top) / rect.height) * 100;
  btn.style.setProperty('--x', `${x}%`);
  btn.style.setProperty('--y', `${y}%`);
}

@Component({
  selector: 'app-hero-section',
  template: `
    <section id="hero" class="hero-section">
      <canvas #heroCanvas id="heroCanvas"></canvas>

      <div class="pill pill-1"></div>
      <div class="pill pill-2"></div>
      <div class="pill pill-3"></div>
      <div class="pill pill-4"></div>
      <div class="pill pill-5"></div>
      <div class="pill pill-6"></div>

      <div class="hero-content">
        <div style="margin-bottom: 1.5rem;">
          <span class="badge-label">
            <span style="color: #2D9CDB;">●</span>
            ثقتك في صحتك منذ أكثر من 20 عاماً
          </span>
        </div>

        <h1 #titleRef style="font-size: clamp(2rem, 5vw, 3.8rem); font-weight: 900; line-height: 1.3; color: #ffffff; margin-bottom: 1.5rem; letter-spacing: 0.02em;">
          الأمين قروب — ثقتك في صحتك
        </h1>

        <p #subtitleRef style="font-size: clamp(1rem, 2.5vw, 1.25rem); color: rgba(184,228,249,0.85); margin-bottom: 2.5rem; font-weight: 400; line-height: 1.8; max-width: 650px; margin-left: auto; margin-right: auto; opacity: 0; transform: translateY(30px); transition: opacity 0.9s ease, transform 0.9s ease;">
          نوفر أفضل الأدوية والمستلزمات الطبية بأعلى معايير الجودة العالمية،
          مع التزامنا الكامل بصحتك وسلامة عائلتك
        </p>

        <div #btnGroupRef style="display: flex; gap: 1.2rem; justify-content: center; flex-wrap: wrap; opacity: 0; transform: translateY(20px); transition: opacity 0.7s ease, transform 0.7s ease;">
          <button class="btn-primary" style="font-size: 1.05rem; padding: 0.95rem 2.4rem;" (click)="onClickRipple($event, '#products')" (mousemove)="addLightFollow($event)">اكتشف منتجاتنا</button>
          <button class="btn-secondary" style="font-size: 1.05rem; padding: 0.95rem 2.4rem;" (click)="onClickRipple($event, '#contact')" (mousemove)="addLightFollow($event)">تواصل معنا</button>
        </div>

        <div style="margin-top: 4rem; animation: bounceArrow 2s ease-in-out infinite; color: rgba(184,228,249,0.5); font-size: 1.5rem;">↓</div>
      </div>

      <style>
        @keyframes bounceArrow {
          0%, 100% { transform: translateY(0); opacity: 0.5; }
          50% { transform: translateY(10px); opacity: 1; }
        }
      </style>
    </section>
  `,
  standalone: true,
})
export class HeroSection {
  private canvasRef = viewChild<ElementRef<HTMLCanvasElement>>('heroCanvas');
  private titleRef = viewChild<ElementRef<HTMLHeadingElement>>('titleRef');
  private subtitleRef = viewChild<ElementRef<HTMLParagraphElement>>('subtitleRef');
  private btnGroupRef = viewChild<ElementRef<HTMLDivElement>>('btnGroupRef');
  private destroyRef = inject(DestroyRef);
  private ngZone = inject(NgZone);

  addLightFollow(e: MouseEvent) {
    const btn = e.currentTarget as HTMLButtonElement;
    if (btn) addLightFollow(e, btn);
  }

  onClickRipple(e: MouseEvent, href: string) {
    addRipple(e, e.currentTarget as HTMLButtonElement);
    document.querySelector(href)?.scrollIntoView({ behavior: 'smooth' });
  }

  constructor() {
    afterNextRender(() => {
      this.ngZone.runOutsideAngular(() => {
        const canvas = this.canvasRef()?.nativeElement;
        if (!canvas) return;

        const scene = new THREE.Scene();
        const camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 1000);
        camera.position.z = 5;

        const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true });
        renderer.setSize(window.innerWidth, window.innerHeight);
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

        const count = 180;
        const positions = new Float32Array(count * 3);
        const velocities = new Float32Array(count * 3);
        for (let i = 0; i < count * 3; i++) {
          positions[i] = (Math.random() - 0.5) * 18;
          velocities[i] = (Math.random() - 0.5) * 0.0025;
        }

        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        const material = new THREE.PointsMaterial({ color: 0x2D9CDB, size: 0.055, transparent: true, opacity: 0.75, sizeAttenuation: true });
        const particles = new THREE.Points(geometry, material);
        scene.add(particles);

        const maxLines = 250;
        const linePositions = new Float32Array(maxLines * 6);
        const lineGeometry = new THREE.BufferGeometry();
        lineGeometry.setAttribute('position', new THREE.BufferAttribute(linePositions, 3));
        const lineMaterial = new THREE.LineBasicMaterial({ color: 0x2D9CDB, transparent: true, opacity: 0.1 });
        const lines = new THREE.LineSegments(lineGeometry, lineMaterial);
        scene.add(lines);

        const mouse = { x: 0, y: 0 };
        const onMouseMove = (e: MouseEvent) => {
          mouse.x = (e.clientX / window.innerWidth) * 2 - 1;
          mouse.y = -(e.clientY / window.innerHeight) * 2 + 1;
        };
        window.addEventListener('mousemove', onMouseMove);

        let rafId = 0;
        const animate = () => {
          rafId = requestAnimationFrame(animate);
          const pos = particles.geometry.attributes['position'].array as Float32Array;
          for (let i = 0; i < count; i++) {
            const ix = i * 3, iy = i * 3 + 1, iz = i * 3 + 2;
            pos[ix] += velocities[ix];
            pos[iy] += velocities[iy];
            pos[iz] += velocities[iz];
            if (Math.abs(pos[ix]) > 9) velocities[ix] *= -1;
            if (Math.abs(pos[iy]) > 9) velocities[iy] *= -1;
            if (Math.abs(pos[iz]) > 9) velocities[iz] *= -1;
          }
          particles.geometry.attributes['position'].needsUpdate = true;

          let lineCount = 0;
          const lp = lineGeometry.attributes['position'].array as Float32Array;
          const threshold = 4.0;
          for (let i = 0; i < count && lineCount < maxLines; i++) {
            for (let j = i + 1; j < count && lineCount < maxLines; j++) {
              const dx = pos[i * 3] - pos[j * 3];
              const dy = pos[i * 3 + 1] - pos[j * 3 + 1];
              const dz = pos[i * 3 + 2] - pos[j * 3 + 2];
              const dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
              if (dist < threshold) {
                const li = lineCount * 6;
                lp[li] = pos[i * 3]; lp[li + 1] = pos[i * 3 + 1]; lp[li + 2] = pos[i * 3 + 2];
                lp[li + 3] = pos[j * 3]; lp[li + 4] = pos[j * 3 + 1]; lp[li + 5] = pos[j * 3 + 2];
                lineCount++;
              }
            }
          }
          lineGeometry.setDrawRange(0, lineCount * 2);
          lineGeometry.attributes['position'].needsUpdate = true;

          particles.rotation.y += 0.0006;
          particles.rotation.x += 0.0002;
          camera.position.x += (mouse.x * 0.7 - camera.position.x) * 0.05;
          camera.position.y += (mouse.y * 0.5 - camera.position.y) * 0.05;
          renderer.render(scene, camera);
        };
        animate();

        const onResize = () => {
          camera.aspect = window.innerWidth / window.innerHeight;
          camera.updateProjectionMatrix();
          renderer.setSize(window.innerWidth, window.innerHeight);
        };
        window.addEventListener('resize', onResize);

        this.destroyRef.onDestroy(() => {
          cancelAnimationFrame(rafId);
          window.removeEventListener('mousemove', onMouseMove);
          window.removeEventListener('resize', onResize);
          renderer.dispose();
          geometry.dispose();
          material.dispose();
        });
      });

      // Hero text animation
      const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
      const animateText = async () => {
        const titleEl = this.titleRef()?.nativeElement;
        const subtitleEl = this.subtitleRef()?.nativeElement;
        const btnGroupEl = this.btnGroupRef()?.nativeElement;
        if (!titleEl || !subtitleEl || !btnGroupEl) return;

        const text = titleEl.textContent || '';
        titleEl.innerHTML = '';
        text.split('').forEach((char) => {
          const wrap = document.createElement('span');
          wrap.className = 'hero-char';
          const inner = document.createElement('span');
          inner.className = 'hero-char-inner';
          inner.textContent = char === ' ' ? '\u00A0' : char;
          wrap.appendChild(inner);
          titleEl.appendChild(wrap);
        });

        await delay(300);
        const chars = titleEl.querySelectorAll<HTMLSpanElement>('.hero-char-inner');
        chars.forEach((c, i) => {
          setTimeout(() => { c.style.transform = 'translateY(0)'; }, i * 35);
        });

        await delay(chars.length * 35 + 400);
        subtitleEl.style.opacity = '1';
        subtitleEl.style.transform = 'translateY(0)';

        await delay(300);
        btnGroupEl.style.opacity = '1';
        btnGroupEl.style.transform = 'translateY(0)';
      };
      animateText();
    });
  }
}
