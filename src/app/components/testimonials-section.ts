import { Component, signal, afterNextRender, DestroyRef, inject } from '@angular/core';

const testimonials = [
  { name: 'د. أحمد الرشيدي', role: 'مدير مستشفى الأمل', stars: 5, text: 'الأمين قروب شريكنا الموثوق منذ سنوات. جودة منتجاتهم لا تضاهى وسرعة التوصيل تفوق التوقعات. ننصح بهم لكل المؤسسات الصحية.', avatar: '👨‍⚕️' },
  { name: 'الصيدلانية فاطمة العمري', role: 'مالكة صيدلية العافية', stars: 5, text: 'تعاملنا مع الأمين قروب منذ أكثر من 8 سنوات. التزامهم بالجودة والأسعار التنافسية جعلنا شركاء ناجحين على المدى الطويل.', avatar: '👩‍⚕️' },
  { name: 'م. خالد السالم', role: 'مدير مشتريات مجموعة طبية', stars: 5, text: 'نقدر ثقتنا الكاملة في الأمين قروب. تشكيلة منتجاتهم الواسعة وخدمة العملاء الممتازة تجعلهم الخيار الأول دائماً.', avatar: '👨‍💼' },
  { name: 'د. نورة الحربي', role: 'طبيبة عامة', stars: 5, text: 'أرشح الأمين قروب لكل زملائي الأطباء. منتجاتهم معتمدة ومضمونة، وفريقهم الاستشاري على قدر عالٍ من الاحترافية.', avatar: '👩‍⚕️' },
];

@Component({
  selector: 'app-testimonials-section',
  template: `
    <section id="testimonials" class="testimonials-section">
      <div style="max-width: 1100px; margin: 0 auto; padding: 0 2rem;">
        <div style="text-align: center; margin-bottom: 4rem;">
          <span class="badge-label">آراء العملاء</span>
          <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: var(--color-text);">
            <div class="section-title-wrap">
              <span #titleRef class="section-title-text">ماذا يقول عملاؤنا</span>
            </div>
          </h2>
        </div>

        <div style="position: relative; margin-bottom: 2.5rem;">
          <div [style.opacity]="isAnimating() ? 0 : 1" [style.transform]="isAnimating() ? 'translateY(10px)' : 'translateY(0)'" style="background: rgba(255,255,255,0.85); backdrop-filter: blur(12px); border: 1px solid rgba(184,228,249,0.4); border-radius: 24px; padding: clamp(2rem, 5vw, 3.5rem); text-align: center; box-shadow: 0 20px 60px rgba(45,156,219,0.12); transition: opacity 0.4s ease, transform 0.4s ease; position: relative; overflow: hidden;">
            <div style="position: absolute; top: -60px; left: -60px; width: 200px; height: 200px; border-radius: 50%; background: radial-gradient(circle, rgba(45,156,219,0.06) 0%, transparent 70%); pointer-events: none;"></div>

            <div style="width: 80px; height: 80px; border-radius: 50%; background: linear-gradient(135deg, rgba(45,156,219,0.15), rgba(184,228,249,0.1)); border: 2px solid rgba(45,156,219,0.3); display: flex; align-items: center; justify-content: center; font-size: 2.5rem; margin: 0 auto 1.2rem; box-shadow: 0 6px 24px rgba(45,156,219,0.2);">{{ testimonials[current()].avatar }}</div>

            <div class="stars" style="margin-bottom: 1rem;">{{ '★'.repeat(testimonials[current()].stars) }}</div>

            <p style="font-size: clamp(1rem, 2.5vw, 1.15rem); color: rgba(26,60,94,0.8); line-height: 1.9; max-width: 650px; margin: 0 auto 1.5rem; font-style: italic;">"{{ testimonials[current()].text }}"</p>

            <div>
              <div style="font-weight: 800; font-size: 1.05rem; color: var(--color-text);">{{ testimonials[current()].name }}</div>
              <div style="font-size: 0.85rem; color: var(--color-accent); font-weight: 500; margin-top: 0.2rem;">{{ testimonials[current()].role }}</div>
            </div>
          </div>
        </div>

        <div style="display: flex; justify-content: center; gap: 0.6rem; margin-bottom: 2rem;">
          @for (t of testimonials; track t.name; let i = $index) {
            <button (click)="goTo(i)" [style.width]="i === current() ? '2rem' : '0.55rem'" [style.height]="'0.55rem'" [style.border-radius]="'50px'" [style.border]="'none'" [style.background]="i === current() ? 'var(--color-accent)' : 'rgba(45,156,219,0.25)'" [style.cursor]="'none'" [style.transition]="'all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)'" [style.box-shadow]="i === current() ? '0 2px 12px rgba(45,156,219,0.5)' : 'none'"></button>
          }
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem;">
          @for (t of testimonials; track t.name; let i = $index) {
            <div (click)="goTo(i)" [style.background]="i === current() ? 'rgba(45,156,219,0.1)' : 'rgba(255,255,255,0.6)'" [style.border]="'1px solid ' + (i === current() ? 'rgba(45,156,219,0.4)' : 'rgba(184,228,249,0.3)')" [style.border-radius]="'14px'" [style.padding]="'1rem 1.2rem'" [style.cursor]="'none'" [style.transition]="'all 0.3s ease'" [style.transition-delay]="i * 0.1 + 's'" [style.backdrop-filter]="'blur(8px)'" class="reveal-up">
              <div style="display: flex; align-items: center; gap: 0.7rem;">
                <span style="font-size: 1.5rem;">{{ t.avatar }}</span>
                <div>
                  <div style="font-weight: 700; font-size: 0.88rem; color: var(--color-text);">{{ t.name }}</div>
                  <div class="stars" style="font-size: 0.8rem;">{{ '★'.repeat(t.stars) }}</div>
                </div>
              </div>
            </div>
          }
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class TestimonialsSection {
  protected readonly testimonials = testimonials;
  protected readonly current = signal(0);
  protected readonly isAnimating = signal(false);
  private destroyRef = inject(DestroyRef);
  private autoInterval: ReturnType<typeof setInterval> | null = null;

  goTo(index: number) {
    if (this.isAnimating() || index === this.current()) return;
    this.isAnimating.set(true);
    this.current.set(index);
    setTimeout(() => this.isAnimating.set(false), 500);
    if (this.autoInterval) clearInterval(this.autoInterval);
    this.autoInterval = setInterval(() => {
      this.current.update((prev) => (prev + 1) % testimonials.length);
    }, 5000);
  }

  constructor() {
    afterNextRender(() => {
      const titleObserver = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) {
            document.querySelector('#testimonials .section-title-text')?.classList.add('revealed');
            titleObserver.disconnect();
          }
        },
        { threshold: 0.3 }
      );
      const titleEl = document.querySelector('#testimonials .section-title-text');
      if (titleEl) titleObserver.observe(titleEl);

      const cardObserver = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              (entry.target as HTMLElement).classList.add('visible');
            }
          });
        },
        { threshold: 0.1 }
      );
      document.querySelectorAll('#testimonials .reveal-up').forEach((el) => cardObserver.observe(el));

      this.autoInterval = setInterval(() => {
        this.current.update((prev) => (prev + 1) % testimonials.length);
      }, 5000);

      this.destroyRef.onDestroy(() => {
        titleObserver.disconnect();
        cardObserver.disconnect();
        if (this.autoInterval) clearInterval(this.autoInterval);
      });
    });
  }
}
