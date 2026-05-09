import { Component, signal, afterNextRender } from '@angular/core';

const contactInfo = [
  { icon: '📍', label: 'العنوان', value: 'الرياض، المملكة العربية السعودية، حي الملز' },
  { icon: '📞', label: 'الهاتف', value: '+966 11 234 5678' },
  { icon: '📧', label: 'البريد الإلكتروني', value: 'info@alamin-group.com' },
  { icon: '🕐', label: 'ساعات العمل', value: 'الأحد - الخميس: 8ص - 6م' },
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
  selector: 'app-contact-section',
  template: `
    <section id="contact" class="contact-section">
      <div style="max-width: 1200px; margin: 0 auto; padding: 0 2rem;">
        <div style="text-align: center; margin-bottom: 4rem;">
          <span class="badge-label">اتصل بنا</span>
          <h2 style="font-size: clamp(1.8rem, 4vw, 2.8rem); font-weight: 900; color: var(--color-text);">
            <div class="section-title-wrap">
              <span #titleRef class="section-title-text">تواصل معنا اليوم</span>
            </div>
          </h2>
          <p style="color: rgba(26,60,94,0.65); font-size: 1rem; max-width: 500px; margin: 1rem auto 0;">فريقنا جاهز للإجابة على جميع استفساراتكم وتقديم أفضل الحلول</p>
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 3rem; align-items: start;">
          <div #formRef class="reveal-right glass-card" style="padding: 2.5rem;">
            <h3 style="font-weight: 800; font-size: 1.3rem; color: var(--color-text); margin-bottom: 1.8rem;">أرسل رسالتك</h3>

            @if (sent()) {
              <div style="background: rgba(39,174,96,0.1); border: 1px solid rgba(39,174,96,0.3); border-radius: 12px; padding: 1rem 1.2rem; margin-bottom: 1.5rem; color: #27AE60; font-weight: 600; font-size: 0.95rem; display: flex; align-items: center; gap: 0.6rem;">✅ تم إرسال رسالتك بنجاح! سنتواصل معك قريباً.</div>
            }

            <form (ngSubmit)="handleSubmit()">
              <div class="floating-input-wrap">
                <input type="text" class="floating-input" placeholder=" " [value]="form().name" (input)="updateField('name', $event)" required />
                <label class="floating-label">الاسم الكامل *</label>
              </div>

              <div class="floating-input-wrap">
                <input type="email" class="floating-input" placeholder=" " [value]="form().email" (input)="updateField('email', $event)" required />
                <label class="floating-label">البريد الإلكتروني *</label>
              </div>

              <div class="floating-input-wrap">
                <input type="tel" class="floating-input" placeholder=" " [value]="form().phone" (input)="updateField('phone', $event)" style="direction: ltr; text-align: right;" />
                <label class="floating-label">رقم الهاتف (اختياري)</label>
              </div>

              <div class="floating-input-wrap">
                <textarea class="floating-input floating-textarea" placeholder=" " [value]="form().message" (input)="updateField('message', $event)" required></textarea>
                <label class="floating-label">رسالتك *</label>
              </div>

              <button type="submit" class="btn-primary" style="width: 100%; font-size: 1.05rem; padding: 1rem;" (click)="addRipple($event)">إرسال الرسالة ✉️</button>
            </form>
          </div>

          <div #infoRef class="reveal-left">
            <h3 style="font-weight: 800; font-size: 1.3rem; color: var(--color-text); margin-bottom: 1.8rem;">معلومات التواصل</h3>

            @for (info of contactInfo; track info.label; let i = $index) {
              <div class="contact-info-item" [style.transition-delay]="i * 0.1 + 's'">
                <div class="contact-icon">{{ info.icon }}</div>
                <div>
                  <div style="font-size: 0.78rem; color: rgba(26,60,94,0.5); font-weight: 600; margin-bottom: 0.15rem;">{{ info.label }}</div>
                  <div style="font-weight: 600; color: var(--color-text); font-size: 0.95rem;">{{ info.value }}</div>
                </div>
              </div>
            }

            <div style="margin-top: 1.5rem; border-radius: 16px; overflow: hidden; border: 1px solid rgba(184,228,249,0.35); background: linear-gradient(135deg, rgba(45,156,219,0.06), rgba(184,228,249,0.04)); height: 180px; display: flex; align-items: center; justify-content: center; flex-direction: column; gap: 0.5rem;">
              <span style="font-size: 3rem;">📍</span>
              <span style="color: rgba(26,60,94,0.5); font-size: 0.9rem;">الرياض، المملكة العربية السعودية</span>
            </div>

            <div style="margin-top: 1.5rem;">
              <p style="color: rgba(26,60,94,0.6); font-size: 0.9rem; margin-bottom: 0.8rem;">تابعونا على:</p>
              <div style="display: flex; gap: 0.7rem;">
                <button class="social-btn" title="Facebook">📘</button>
                <button class="social-btn" title="Instagram">📸</button>
                <button class="social-btn" title="Twitter">🐦</button>
                <button class="social-btn" title="LinkedIn">💼</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  `,
  standalone: true,
})
export class ContactSection {
  protected readonly contactInfo = contactInfo;
  protected readonly form = signal({ name: '', email: '', phone: '', message: '' });
  protected readonly sent = signal(false);

  addRipple(e: MouseEvent) {
    addRipple(e);
  }

  updateField(field: string, event: Event) {
    const value = (event.target as HTMLInputElement).value;
    this.form.update((f) => ({ ...f, [field]: value }));
  }

  handleSubmit() {
    this.sent.set(true);
    this.form.set({ name: '', email: '', phone: '', message: '' });
    setTimeout(() => this.sent.set(false), 4000);
  }

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
      document.querySelectorAll('#contact .section-title-text, #contact .reveal-right, #contact .reveal-left').forEach((el) => observer.observe(el));
    });
  }
}
