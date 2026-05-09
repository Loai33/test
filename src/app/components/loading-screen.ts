import { Component, output, viewChild, ElementRef, DestroyRef, inject } from '@angular/core';

@Component({
  selector: 'app-loading-screen',
  template: `
    <div #overlay class="loader-overlay">
      <div class="loader-particles">
        @for (p of particles; track p.id) {
          <div class="loader-particle" [style.width.px]="p.size" [style.height.px]="p.size" [style.left.%]="p.left" [style.top.%]="p.top" [style.--dur]="p.dur" [style.--delay]="p.delay"></div>
        }
      </div>

      <div style="text-align: center; position: relative; z-index: 2;">
        <div style="font-size: 3.5rem; margin-bottom: 1rem; animation: iconFloat 2s ease-in-out infinite; display: inline-block;">💊</div>
        <div class="loader-logo">الأمين قروب</div>
        <div class="loader-subtitle">للأدوية والمستلزمات الطبية</div>
        <div class="loader-bar-container">
          <div class="loader-bar"></div>
        </div>
        <div class="loader-text">جاري التحميل...</div>
      </div>

      <style>
        @keyframes iconFloat {
          0%, 100% { transform: translateY(0) rotate(0deg); }
          50% { transform: translateY(-10px) rotate(5deg); }
        }
      </style>
    </div>
  `,
  standalone: true,
})
export class LoadingScreen {
  readonly onDone = output<void>();
  private overlay = viewChild<ElementRef<HTMLDivElement>>('overlay');
  private destroyRef = inject(DestroyRef);

  readonly particles = Array.from({ length: 18 }, (_, i) => ({
    id: i,
    size: Math.random() * 6 + 3,
    left: Math.random() * 100,
    top: Math.random() * 100,
    dur: `${Math.random() * 6 + 5}s`,
    delay: `${Math.random() * 4}s`,
  }));

  constructor() {
    const timer = setTimeout(() => {
      const overlayEl = this.overlay()?.nativeElement;
      if (overlayEl) {
        overlayEl.classList.add('hidden');
        setTimeout(() => this.onDone.emit(), 900);
      }
    }, 2400);
    this.destroyRef.onDestroy(() => clearTimeout(timer));
  }
}
