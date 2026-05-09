import { Component, signal, effect, afterNextRender, DestroyRef, inject } from '@angular/core';

const categories = ['الكل', 'مضادات حيوية', 'مكملات غذائية', 'أدوية قلب', 'جهاز هضمي'];

const products = [
  { id: 1, icon: '💊', name: 'أميكسيلين 500mg', desc: 'مضاد حيوي واسع المجال لعلاج الالتهابات البكتيرية', category: 'مضادات حيوية', badge: 'الأكثر مبيعاً' },
  { id: 2, icon: '🧴', name: 'فيتامين D3 + K2', desc: 'مكمل غذائي متكامل لصحة العظام والمناعة', category: 'مكملات غذائية', badge: 'جديد' },
  { id: 3, icon: '❤️', name: 'كارديوفاست 10mg', desc: 'لعلاج ارتفاع ضغط الدم وأمراض القلب', category: 'أدوية قلب', badge: null },
  { id: 4, icon: '🌿', name: 'بروبيوتيك بلس', desc: 'تركيبة متطورة لصحة الجهاز الهضمي والأمعاء', category: 'جهاز هضمي', badge: null },
  { id: 5, icon: '💉', name: 'أوميغا 3 فيش أويل', desc: 'زيت السمك النقي لصحة القلب والمفاصل والدماغ', category: 'مكملات غذائية', badge: 'الأكثر مبيعاً' },
  { id: 6, icon: '🔬', name: 'ميترونيدازول 400mg', desc: 'لعلاج الالتهابات والعدوى الجرثومية المعوية', category: 'مضادات حيوية', badge: null },
];

function addRipple(e: MouseEvent) {
  const btn = e.currentTarget as HTMLButtonElement;
  const rect = btn.getBoundingClientRect();
  const ripple = document.createElement('span');
  ripple.className = 'ripple';
  const size = Math.max(rect.width, rect.height);
  ripple.style.cssText = `width:${size}px;height:${size}px;left:${e.clientX - rect.left - size / 2}px;top:${e.clientY - rect.top - size / 2}px;`;
  btn.appendChild(ripple);
  setTimeout(() => ripple.remove(), 700);
}

@Component({
  selector: 'app-products-section',
  template: `
    <section id="products" class="products-section">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="text-align: center; margin-bottom: 3rem;">
          <span class="badge-label">منتجاتنا</span>
          <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: var(--color-text); margin-bottom: 1rem;">
            <div class="section-title-wrap">
              <span #titleRef class="section-title-text">منتجاتنا الدوائية المتميزة</span>
            </div>
          </h2>
          <p style="color: rgba(26,60,94,0.65); font-size: 1rem; max-width: 550px; margin: 0 auto;">نوفر مجموعة واسعة من الأدوية والمستلزمات الطبية بأعلى معايير الجودة</p>
        </div>

        <div style="display: flex; gap: 0.75rem; justify-content: center; flex-wrap: wrap; margin-bottom: 3rem;">
          @for (cat of categories; track cat) {
            <button class="filter-btn" [class.active]="activeFilter() === cat" (click)="setFilter(cat); addRipple($event)">{{ cat }}</button>
          }
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 1.8rem;">
          @for (product of filtered(); track product.id; let i = $index) {
            <div class="glass-card reveal-up" [style.transition-delay]="i * 0.1 + 's'" style="padding: 2rem;">
              @if (product.badge) {
                <div style="position: absolute; top: 1rem; left: 1rem; background: linear-gradient(135deg, var(--color-accent), var(--color-accent-dark)); color: white; font-size: 0.72rem; font-weight: 700; padding: 0.2rem 0.8rem; border-radius: 50px; box-shadow: 0 3px 12px rgba(45,156,219,0.4);">{{ product.badge }}</div>
              }
              <div class="product-icon">{{ product.icon }}</div>
              <h3 style="font-size: 1.15rem; font-weight: 700; color: var(--color-text); margin-bottom: 0.6rem;">{{ product.name }}</h3>
              <span style="display: inline-block; font-size: 0.75rem; color: var(--color-accent); background: rgba(45,156,219,0.08); border: 1px solid rgba(45,156,219,0.2); border-radius: 50px; padding: 0.15rem 0.8rem; margin-bottom: 0.8rem; font-weight: 600;">{{ product.category }}</span>
              <p style="font-size: 0.9rem; color: rgba(26,60,94,0.65); line-height: 1.7; margin-bottom: 1.5rem;">{{ product.desc }}</p>
              <button class="btn-accent" style="width: 100%;" (click)="addRipple($event)">التفاصيل ←</button>
            </div>
          }
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class ProductsSection {
  protected readonly categories = categories;
  protected readonly products = products;
  protected readonly activeFilter = signal('الكل');
  protected readonly filtered = signal([...products]);
  private destroyRef = inject(DestroyRef);

  addRipple(e: MouseEvent) {
    addRipple(e);
  }

  setFilter(cat: string) {
    this.activeFilter.set(cat);
    this.filtered.set(
      cat === 'الكل' ? [...products] : products.filter((p) => p.category === cat)
    );
  }

  constructor() {
    afterNextRender(() => {
      const titleObserver = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) {
            document.querySelector('#products .section-title-text')?.classList.add('revealed');
            titleObserver.disconnect();
          }
        },
        { threshold: 0.3 }
      );
      const titleEl = document.querySelector('#products .section-title-text');
      if (titleEl) titleObserver.observe(titleEl);

      effect((onCleanup) => {
        this.filtered();
        let cardObserver: IntersectionObserver | null = null;
        const timer = setTimeout(() => {
          cardObserver = new IntersectionObserver(
            (entries) => {
              entries.forEach((entry) => {
                if (entry.isIntersecting) {
                  (entry.target as HTMLElement).classList.add('visible');
                  cardObserver?.unobserve(entry.target);
                }
              });
            },
            { threshold: 0.1 }
          );
          document.querySelectorAll('#products .glass-card.reveal-up').forEach((el) => {
            el.classList.remove('visible');
            cardObserver?.observe(el);
          });
        }, 50);

        onCleanup(() => {
          clearTimeout(timer);
          cardObserver?.disconnect();
        });
      });

      this.destroyRef.onDestroy(() => titleObserver.disconnect());
    });
  }
}
