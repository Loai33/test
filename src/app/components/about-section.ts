import { Component, afterNextRender } from '@angular/core';

const features = [
  { icon: '✅', title: 'جودة معتمدة دولياً', desc: 'نلتزم بأعلى معايير الجودة العالمية في جميع منتجاتنا' },
  { icon: '🚚', title: 'توزيع سريع وموثوق', desc: 'شبكة توزيع واسعة تغطي جميع أنحاء البلاد' },
  { icon: '🔬', title: 'أبحاث متطورة', desc: 'نستثمر في البحث والتطوير لتوفير أحدث الأدوية' },
  { icon: '🤝', title: 'شراكات استراتيجية', desc: 'تعاون مع أكبر شركات الأدوية العالمية' },
];

@Component({
  selector: 'app-about-section',
  template: `
    <section id="about" class="about-section">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="text-align: center; margin-bottom: 4rem;">
          <span class="badge-label">من نحن</span>
          <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: var(--color-text);">
            <div class="section-title-wrap">
              <span #titleRef class="section-title-text">رواد صناعة الأدوية في المنطقة</span>
            </div>
          </h2>
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 4rem; align-items: center;">
          <div #textRef class="reveal-right" style="order: 1;">
            <p style="font-size: 1.05rem; line-height: 2; color: rgba(26,60,94,0.8); margin-bottom: 2rem;">
              تأسس الأمين قروب للأدوية منذ أكثر من <strong style="color: var(--color-accent);">20 عاماً</strong>،
              وأصبح اليوم من أبرز شركات توزيع الأدوية في المنطقة.
              نفخر بتوفير أكثر من <strong style="color: var(--color-accent);">500 منتج دوائي</strong> بأعلى
              معايير الجودة والسلامة، خدمةً لأكثر من ألف عميل في مختلف القطاعات الصحية.
            </p>

            <div>
              @for (feature of features; track feature.title; let i = $index) {
                <div class="feature-item" [style.transition-delay]="i * 0.12 + 's'" style="display: flex; align-items: flex-start; gap: 1rem; margin-bottom: 1.5rem;">
                  <div class="feature-icon-wrap">
                    <span style="font-size: 1.2rem;">{{ feature.icon }}</span>
                  </div>
                  <div>
                    <div style="font-weight: 700; color: var(--color-text); margin-bottom: 0.2rem;">{{ feature.title }}</div>
                    <div style="font-size: 0.9rem; color: rgba(26,60,94,0.65); line-height: 1.6;">{{ feature.desc }}</div>
                  </div>
                </div>
              }
            </div>

            <button class="btn-primary" style="margin-top: 2rem;" (click)="scrollTo('#products')">اكتشف منتجاتنا</button>
          </div>

          <div #imageRef class="reveal-left" style="order: 2; display: flex; justify-content: center;">
            <div class="orbit-container">
              <div class="orbit-core">💊</div>

              <div class="orbit-ring orbit-ring-1">
                <div class="orbit-item" style="position: absolute; top: -22px; left: 50%; margin-left: -22px; animation: spin 12s linear infinite reverse;">🧬</div>
                <div class="orbit-item" style="position: absolute; top: 50%; left: -22px; margin-top: -22px; animation: spin 14s linear infinite reverse;">⚗️</div>
                <div class="orbit-item" style="position: absolute; bottom: -22px; left: 50%; margin-left: -22px; animation: spin 16s linear infinite reverse;">🩺</div>
                <div class="orbit-item" style="position: absolute; top: 50%; right: -22px; margin-top: -22px; animation: spin 18s linear infinite reverse;">💉</div>
              </div>

              <div class="orbit-ring orbit-ring-2">
                <div class="orbit-item" style="position: absolute; top: -22px; left: 50%; margin-left: -22px; animation: spin 20s linear infinite;">🏥</div>
                <div class="orbit-item" style="position: absolute; bottom: -22px; left: 50%; margin-left: -22px; animation: spin 23s linear infinite;">🩻</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class AboutSection {
  protected readonly features = features;

  constructor() {
    afterNextRender(() => {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              entry.target.classList.add('visible', 'revealed');
              observer.unobserve(entry.target);
            }
          });
        },
        { threshold: 0.15 }
      );

      document.querySelectorAll('#about .section-title-text, #about .reveal-right, #about .reveal-left').forEach((el) => observer.observe(el));

      const featureObserver = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              (entry.target as HTMLElement).classList.add('visible');
              featureObserver.unobserve(entry.target);
            }
          });
        },
        { threshold: 0.15 }
      );

      document.querySelectorAll('#about .feature-item').forEach((el) => featureObserver.observe(el));
    });
  }

  scrollTo(href: string) {
    document.querySelector(href)?.scrollIntoView({ behavior: 'smooth' });
  }
}
