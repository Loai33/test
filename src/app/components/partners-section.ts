import { Component, afterNextRender, viewChild, ElementRef, DestroyRef, inject } from '@angular/core';

const partners = [
  { name: 'PharmaCo', icon: '💊', country: 'ألمانيا' },
  { name: 'MediLife', icon: '🏥', country: 'فرنسا' },
  { name: 'BioCure', icon: '🧬', country: 'الولايات المتحدة' },
  { name: 'HealthPlus', icon: '➕', country: 'بريطانيا' },
  { name: 'VitaGroup', icon: '🌿', country: 'سويسرا' },
  { name: 'NovaMed', icon: '⚕️', country: 'إيطاليا' },
  { name: 'GenoPharma', icon: '🔬', country: 'كندا' },
  { name: 'SafeRx', icon: '🛡️', country: 'هولندا' },
];

const doubled = [...partners, ...partners];

@Component({
  selector: 'app-partners-section',
  template: `
    <section id="partners" class="partners-section">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem; text-align: center;">
        <span class="badge-label" style="color: rgba(184,228,249,0.8); border-color: rgba(184,228,249,0.25); background: rgba(184,228,249,0.05);">شركاؤنا</span>
        <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: #B8E4F9; margin-bottom: 1rem;">
          <div class="section-title-wrap">
            <span #titleRef class="section-title-text">شركاؤنا حول العالم</span>
          </div>
        </h2>
        <p style="color: rgba(184,228,249,0.6); font-size: 1rem; max-width: 550px; margin: 0 auto 3.5rem;">
          نتعاون مع أبرز شركات الأدوية العالمية لنوفر لكم أفضل المنتجات
        </p>
      </div>

      <div style="overflow: hidden; padding: 1rem 0;">
        <div class="marquee-track">
          @for (partner of doubled; track partner.name) {
            <div class="partner-logo-item">
              <span style="font-size: 1.8rem; filter: drop-shadow(0 0 8px rgba(45,156,219,0.5));">{{ partner.icon }}</span>
              <div>
                <div style="color: #B8E4F9; font-weight: 700; font-size: 0.95rem;">{{ partner.name }}</div>
                <div style="color: rgba(184,228,249,0.5); font-size: 0.75rem;">{{ partner.country }}</div>
              </div>
            </div>
          }
        </div>
      </div>

      <div style="max-width: 900px; margin: 4rem auto 0; padding: 0 2rem; display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 2rem; text-align: center;">
        <div>
          <div style="font-size: 2rem; font-weight: 900; background: linear-gradient(135deg, #B8E4F9, #2D9CDB); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; margin-bottom: 0.4rem;">50+</div>
          <div style="color: rgba(184,228,249,0.6); font-size: 0.9rem;">دولة حول العالم</div>
        </div>
        <div>
          <div style="font-size: 2rem; font-weight: 900; background: linear-gradient(135deg, #B8E4F9, #2D9CDB); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; margin-bottom: 0.4rem;">15+</div>
          <div style="color: rgba(184,228,249,0.6); font-size: 0.9rem;">سنة شراكة متواصلة</div>
        </div>
        <div>
          <div style="font-size: 2rem; font-weight: 900; background: linear-gradient(135deg, #B8E4F9, #2D9CDB); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; margin-bottom: 0.4rem;">99%</div>
          <div style="color: rgba(184,228,249,0.6); font-size: 0.9rem;">رضا الشركاء</div>
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class PartnersSection {
  protected readonly doubled = doubled;
  private titleRef = viewChild<ElementRef<HTMLSpanElement>>('titleRef');
  private destroyRef = inject(DestroyRef);

  constructor() {
    afterNextRender(() => {
      const observer = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) {
            this.titleRef()?.nativeElement.classList.add('revealed');
            observer.disconnect();
          }
        },
        { threshold: 0.3 }
      );
      const el = this.titleRef()?.nativeElement;
      if (el) observer.observe(el);
      this.destroyRef.onDestroy(() => observer.disconnect());
    });
  }
}
