import { Component, afterNextRender, viewChild, ElementRef, DestroyRef, inject } from '@angular/core';

const services = [
  {
    icon: '🚚',
    title: 'التوزيع الدوائي',
    desc: 'شبكة توزيع متطورة تضمن وصول الأدوية في الوقت المناسب وبأعلى معايير السلامة والجودة إلى جميع نقاط البيع.',
    features: ['تغطية شاملة', 'توصيل مبرد', 'تتبع مباشر'],
    color: '#2D9CDB',
    delay: 0,
  },
  {
    icon: '🔬',
    title: 'الاستشارات الطبية',
    desc: 'فريق من الصيادلة والمختصين الطبيين يقدم استشارات متخصصة ودعماً علمياً متواصلاً للعملاء والمرضى.',
    features: ['24/7 دعم', 'استشارة مجانية', 'خبراء معتمدون'],
    color: '#27AE60',
    delay: 0.15,
  },
  {
    icon: '🏆',
    title: 'ضمان الجودة',
    desc: 'نلتزم بمعايير جودة عالمية صارمة في كل مراحل التخزين والتوزيع، مع شهادات اعتماد من أبرز الهيئات الدولية.',
    features: ['معتمد دولياً', 'فحص دوري', 'شهادات جودة'],
    color: '#F2994A',
    delay: 0.3,
  },
];

@Component({
  selector: 'app-services-section',
  template: `
    <section id="services" class="services-section">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="text-align: center; margin-bottom: 4rem;">
          <span class="badge-label">خدماتنا</span>
          <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: var(--color-text); margin-bottom: 1rem;">
            <div class="section-title-wrap">
              <span #titleRef class="section-title-text">خدمات متكاملة لقطاع الصحة</span>
            </div>
          </h2>
          <p style="color: rgba(26,60,94,0.65); font-size: 1rem; max-width: 550px; margin: 0 auto;">نقدم حلولاً شاملة تغطي كل احتياجات قطاع الأدوية والرعاية الصحية</p>
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 2rem;">
          @for (service of services; track service.title; let i = $index) {
            <div class="glass-card reveal-up" #cardsRef [style.transition-delay]="service.delay + 's'" style="padding: 2.5rem 2rem; text-align: center;">
              <div class="service-icon-animated" [style.animation-delay]="i + 's'" [style.background]="'linear-gradient(135deg, ' + service.color + ', ' + service.color + 'bb)'">
                {{ service.icon }}
              </div>
              <h3 style="font-size: 1.35rem; font-weight: 800; color: var(--color-text); margin-bottom: 1rem;">{{ service.title }}</h3>
              <p style="font-size: 0.95rem; color: rgba(26,60,94,0.65); line-height: 1.8; margin-bottom: 1.5rem;">{{ service.desc }}</p>
              <div style="display: flex; gap: 0.6rem; justify-content: center; flex-wrap: wrap;">
                @for (f of service.features; track f) {
                  <span style="font-size: 0.78rem; font-weight: 600; padding: 0.25rem 0.85rem; border-radius: 50px; background: {{ service.color }}15; border: 1px solid {{ service.color }}35; color: {{ service.color }};">{{ f }}</span>
                }
              </div>
            </div>
          }
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class ServicesSection {
  protected readonly services = services;
  private titleRef = viewChild<ElementRef<HTMLSpanElement>>('titleRef');
  private cardsRef = viewChild<ElementRef<HTMLDivElement>>('cardsRef');
  private destroyRef = inject(DestroyRef);

  constructor() {
    afterNextRender(() => {
      const titleObserver = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) {
            this.titleRef()?.nativeElement.classList.add('revealed');
            titleObserver.disconnect();
          }
        },
        { threshold: 0.3 }
      );
      const titleEl = this.titleRef()?.nativeElement;
      if (titleEl) titleObserver.observe(titleEl);

      const cardObserver = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              (entry.target as HTMLElement).classList.add('visible');
              cardObserver.unobserve(entry.target);
            }
          });
        },
        { threshold: 0.15 }
      );
      document.querySelectorAll('.glass-card.reveal-up').forEach((el) => cardObserver.observe(el));

      this.destroyRef.onDestroy(() => {
        titleObserver.disconnect();
        cardObserver.disconnect();
      });
    });
  }
}
