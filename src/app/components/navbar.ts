import { Component, signal, HostListener } from '@angular/core';

const navLinks = [
  { href: '#hero', label: 'الرئيسية' },
  { href: '#about', label: 'من نحن' },
  { href: '#products', label: 'منتجاتنا' },
  { href: '#services', label: 'خدماتنا' },
  { href: '#partners', label: 'الشركاء' },
  { href: '#contact', label: 'اتصل بنا' },
];

@Component({
  selector: 'app-navbar',
  template: `
    <nav class="navbar" [class.scrolled]="scrolled()" [style.background]="scrolled() ? undefined : 'transparent'">
      <div style="max-width: 1300px; margin: 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 2rem;">
        <a href="#hero" (click)="scrollTo($event, '#hero')" style="display: flex; align-items: center; gap: 0.6rem; text-decoration: none; flex-shrink: 0;">
          <div style="width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, #2D9CDB, #1a7ab5); display: flex; align-items: center; justify-content: center; font-size: 1.3rem; box-shadow: 0 4px 15px rgba(45,156,219,0.45);">💊</div>
          <div>
            <div style="color: #B8E4F9; font-weight: 900; font-size: 1.05rem; line-height: 1.2; letter-spacing: 0.02em;">الأمين قروب</div>
            <div style="color: rgba(184,228,249,0.55); font-size: 0.65rem; font-weight: 400;">للأدوية والمستلزمات الطبية</div>
          </div>
        </a>

        <div style="display: flex; gap: 2rem; align-items: center;" class="desktop-nav">
          @for (link of navLinks; track link.href) {
            <a [href]="link.href" class="nav-link" (click)="scrollTo($event, link.href)">{{ link.label }}</a>
          }
        </div>

        <a href="#contact" (click)="scrollTo($event, '#contact')" class="btn-primary desktop-nav" style="font-size: 0.9rem; padding: 0.65rem 1.6rem;">تواصل معنا</a>

        <button (click)="toggleMobile()" class="mobile-menu-btn" style="background: none; border: 1.5px solid rgba(184,228,249,0.35); border-radius: 10px; padding: 0.45rem 0.7rem; cursor: none; display: none; color: #B8E4F9; font-size: 1.2rem;">{{ mobileOpen() ? '✕' : '☰' }}</button>
      </div>

      @if (mobileOpen()) {
        <div style="position: absolute; top: 100%; right: 0; left: 0; background: rgba(10,22,40,0.97); backdrop-filter: blur(20px); border-top: 1px solid rgba(184,228,249,0.12); padding: 1.5rem; display: flex; flex-direction: column; gap: 0.8rem;">
          @for (link of navLinks; track link.href) {
            <a [href]="link.href" class="nav-link" (click)="scrollTo($event, link.href)" style="padding: 0.5rem 0; font-size: 1rem;">{{ link.label }}</a>
          }
          <a href="#contact" (click)="scrollTo($event, '#contact')" class="btn-primary" style="text-align: center; margin-top: 0.5rem;">تواصل معنا</a>
        </div>
      }

      <style>
        @media (max-width: 900px) {
          .desktop-nav { display: none !important; }
          .mobile-menu-btn { display: block !important; }
        }
      </style>
    </nav>
  `,
  standalone: true,
})
export class Navbar {
  protected readonly navLinks = navLinks;
  protected readonly scrolled = signal(false);
  protected readonly mobileOpen = signal(false);

  @HostListener('window:scroll')
  onScroll() {
    this.scrolled.set(window.scrollY > 60);
  }

  toggleMobile() {
    this.mobileOpen.update(v => !v);
  }

  scrollTo(e: MouseEvent, href: string) {
    e.preventDefault();
    document.querySelector(href)?.scrollIntoView({ behavior: 'smooth' });
    this.mobileOpen.set(false);
  }
}
