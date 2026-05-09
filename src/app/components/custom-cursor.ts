import { Component, viewChild, ElementRef, HostListener, inject, DestroyRef } from '@angular/core';

@Component({
  selector: 'app-custom-cursor',
  template: `
    <div #dot class="cursor-dot"></div>
    <div #ring class="cursor-ring"></div>
  `,
  standalone: true,
})
export class CustomCursor {
  private dot = viewChild<ElementRef<HTMLDivElement>>('dot');
  private ring = viewChild<ElementRef<HTMLDivElement>>('ring');
  private mouseX = 0;
  private mouseY = 0;
  private ringX = 0;
  private ringY = 0;
  private rafId = 0;
  private destroyRef = inject(DestroyRef);

  @HostListener('window:mousemove', ['$event'])
  onMouseMove(e: MouseEvent) {
    this.mouseX = e.clientX;
    this.mouseY = e.clientY;
    this.dot()?.nativeElement.style.setProperty('left', `${e.clientX}px`);
    this.dot()?.nativeElement.style.setProperty('top', `${e.clientY}px`);
  }

  @HostListener('document:mouseover', ['$event'])
  onMouseOver(e: MouseEvent) {
    const target = e.target as HTMLElement;
    const isInteractive = !!target.closest('a, button, .glass-card, .filter-btn, .partner-logo-item, .social-btn, .nav-link, .footer-link, [role="button"]');
    this.dot()?.nativeElement.classList.toggle('hovering', isInteractive);
    this.ring()?.nativeElement.classList.toggle('hovering', isInteractive);
  }

  constructor() {
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    if (isMobile) return;

    const animateRing = () => {
      this.ringX += (this.mouseX - this.ringX) * 0.13;
      this.ringY += (this.mouseY - this.ringY) * 0.13;
      const ringEl = this.ring()?.nativeElement;
      if (ringEl) {
        ringEl.style.left = `${this.ringX}px`;
        ringEl.style.top = `${this.ringY}px`;
      }
      this.rafId = requestAnimationFrame(animateRing);
    };
    this.rafId = requestAnimationFrame(animateRing);

    this.destroyRef.onDestroy(() => {
      cancelAnimationFrame(this.rafId);
    });
  }
}
