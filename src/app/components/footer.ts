import { Component } from '@angular/core';

const quickLinks = [
  { href: '#hero', label: 'الرئيسية' },
  { href: '#about', label: 'من نحن' },
  { href: '#products', label: 'منتجاتنا' },
  { href: '#services', label: 'خدماتنا' },
  { href: '#partners', label: 'الشركاء' },
  { href: '#contact', label: 'اتصل بنا' },
];

const socialLinks = [
  { icon: '📘', name: 'Facebook' },
  { icon: '📸', name: 'Instagram' },
  { icon: '🐦', name: 'Twitter/X' },
  { icon: '💼', name: 'LinkedIn' },
  { icon: '📱', name: 'WhatsApp' },
];

const services = ['التوزيع الدوائي', 'الاستشارات الطبية', 'ضمان الجودة', 'الشراكات الدولية', 'التخزين المبرد', 'الدعم الفني'];

const contactItems = [
  { icon: '📍', text: 'الرياض، المملكة العربية السعودية' },
  { icon: '📞', text: '+966 11 234 5678' },
  { icon: '📧', text: 'info@alamin-group.com' },
  { icon: '🕐', text: 'الأحد - الخميس: 8ص - 6م' },
];

@Component({
  selector: 'app-footer',
  template: `
    <footer class="footer">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 3rem; margin-bottom: 3rem; padding-bottom: 3rem; border-bottom: 1px solid rgba(184,228,249,0.1);">
          <div>
            <div style="display: flex; align-items: center; gap: 0.7rem; margin-bottom: 1.2rem;">
              <div style="width: 44px; height: 44px; border-radius: 12px; background: linear-gradient(135deg, #2D9CDB, #1a7ab5); display: flex; align-items: center; justify-content: center; font-size: 1.4rem; box-shadow: 0 4px 15px rgba(45,156,219,0.4);">💊</div>
              <div>
                <div style="color: #B8E4F9; font-weight: 900; font-size: 1.1rem;">الأمين قروب</div>
                <div style="color: rgba(184,228,249,0.45); font-size: 0.7rem;">للأدوية والمستلزمات الطبية</div>
              </div>
            </div>
            <p style="color: rgba(184,228,249,0.55); font-size: 0.88rem; line-height: 1.9; margin-bottom: 1.5rem;">
              نوفر أفضل الأدوية والمستلزمات الطبية بأعلى معايير الجودة، ملتزمون بصحتك وسلامة عائلتك منذ أكثر من 20 عاماً.
            </p>
            <div style="display: flex; gap: 0.6rem; flex-wrap: wrap;">
              @for (s of socialLinks; track s.name) {
                <button class="social-btn" [title]="s.name" style="width: 36px; height: 36px; font-size: 1rem;">{{ s.icon }}</button>
              }
            </div>
          </div>

          <div>
            <h4 style="color: #B8E4F9; font-weight: 700; font-size: 1rem; margin-bottom: 1.2rem;">روابط سريعة</h4>
            <ul style="list-style: none; padding: 0; margin: 0;">
              @for (link of quickLinks; track link.href) {
                <li style="margin-bottom: 0.7rem;">
                  <a [href]="link.href" class="footer-link" (click)="scrollTo($event, link.href)" style="display: flex; align-items: center; gap: 0.4rem;">
                    <span style="color: var(--color-accent); font-size: 0.7rem;">←</span>{{ link.label }}
                  </a>
                </li>
              }
            </ul>
          </div>

          <div>
            <h4 style="color: #B8E4F9; font-weight: 700; font-size: 1rem; margin-bottom: 1.2rem;">خدماتنا</h4>
            <ul style="list-style: none; padding: 0; margin: 0;">
              @for (service of services; track service) {
                <li style="margin-bottom: 0.7rem;">
                  <span class="footer-link">
                    <span style="color: var(--color-accent); margin-left: 0.4rem;">●</span>{{ service }}
                  </span>
                </li>
              }
            </ul>
          </div>

          <div>
            <h4 style="color: #B8E4F9; font-weight: 700; font-size: 1rem; margin-bottom: 1.2rem;">تواصل معنا</h4>
            @for (item of contactItems; track item.text) {
              <div style="display: flex; align-items: flex-start; gap: 0.6rem; margin-bottom: 0.9rem;">
                <span style="font-size: 0.95rem; flex-shrink: 0;">{{ item.icon }}</span>
                <span style="color: rgba(184,228,249,0.6); font-size: 0.85rem; line-height: 1.6;">{{ item.text }}</span>
              </div>
            }
          </div>
        </div>

        <div style="display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 1rem;">
          <p style="color: rgba(184,228,249,0.4); font-size: 0.82rem;">© {{ currentYear }} الأمين قروب للأدوية — جميع الحقوق محفوظة</p>
          <div style="display: flex; gap: 1.5rem;">
            <a href="#" class="footer-link" style="font-size: 0.8rem;">سياسة الخصوصية</a>
            <a href="#" class="footer-link" style="font-size: 0.8rem;">شروط الاستخدام</a>
            <a href="#" class="footer-link" style="font-size: 0.8rem;">سياسة الإرجاع</a>
          </div>
        </div>
      </div>
    </footer>
  `,
  standalone: true,
})
export class Footer {
  protected readonly quickLinks = quickLinks;
  protected readonly socialLinks = socialLinks;
  protected readonly services = services;
  protected readonly contactItems = contactItems;
  protected readonly currentYear = new Date().getFullYear();

  scrollTo(e: MouseEvent, href: string) {
    e.preventDefault();
    document.querySelector(href)?.scrollIntoView({ behavior: 'smooth' });
  }
}
