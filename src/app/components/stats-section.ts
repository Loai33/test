import { Component, afterNextRender } from '@angular/core';

const stats = [
  { number: 500, label: 'منتج دوائي', icon: '💊', suffix: '+' },
  { number: 20, label: 'سنة خبرة', icon: '📅', suffix: '+' },
  { number: 1000, label: 'عميل راضٍ', icon: '😊', suffix: '+' },
  { number: 50, label: 'شريك دولي', icon: '🌍', suffix: '+' },
];

@Component({
  selector: 'app-stats-section',
  template: `
    <section class="stats-section" style="position: relative; overflow: hidden;">
      <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%); width: 600px; height: 600px; background: radial-gradient(circle, rgba(45,156,219,0.08) 0%, transparent 70%); pointer-events: none;"></div>
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 0;">
          @for (stat of stats; track stat.label; let i = $index) {
            <div class="reveal-up" [style.transition-delay]="i * 0.15 + 's'" style="text-align: center; padding: 2.5rem 2rem; position: relative;">
              @if (i < stats.length - 1) {
                <div style="position: absolute; top: 20%; left: 0; width: 1px; height: 60%; background: rgba(184,228,249,0.15);"></div>
              }
              <div style="font-size: 2.5rem; margin-bottom: 1rem; filter: drop-shadow(0 0 15px rgba(45,156,219,0.6));">{{ stat.icon }}</div>
              <div #counterEl class="stat-number" style="margin-bottom: 0.5rem;">0{{ stat.suffix }}</div>
              <div style="color: rgba(184,228,249,0.7); font-size: 1rem; font-weight: 500;">{{ stat.label }}</div>
            </div>
          }
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class StatsSection {
  protected readonly stats = stats;

  constructor() {
    afterNextRender(() => {
      document.querySelectorAll('.stats-section .reveal-up').forEach((el) => {
        const observer = new IntersectionObserver(
          ([entry]) => {
            if (entry.isIntersecting) {
              el.classList.add('visible');
              observer.disconnect();
            }
          },
          { threshold: 0.2 }
        );
        observer.observe(el);
      });

      const numElements = document.querySelectorAll('.stats-section .stat-number');
      numElements.forEach((el, index) => {
        if (index >= stats.length) return;
        const stat = stats[index];
        const observer = new IntersectionObserver(
          ([entry]) => {
            if (entry.isIntersecting) {
              const target = stat.number;
              const suffix = stat.suffix;
              const duration = 2200;
              const start = performance.now();
              const update = (time: number) => {
                const progress = Math.min((time - start) / duration, 1);
                const eased = 1 - Math.pow(1 - progress, 3);
                el.textContent = Math.round(eased * target) + suffix;
                if (progress < 1) requestAnimationFrame(update);
              };
              requestAnimationFrame(update);
              observer.disconnect();
            }
          },
          { threshold: 0.5 }
        );
        observer.observe(el);
      });
    });
  }
}
