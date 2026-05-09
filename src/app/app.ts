import { Component, signal } from '@angular/core';
import { LoadingScreen } from './components/loading-screen';
import { CustomCursor } from './components/custom-cursor';
import { Navbar } from './components/navbar';
import { HeroSection } from './components/hero-section';
import { StatsSection } from './components/stats-section';
import { WaveSeparator } from './components/wave-separator';
import { AboutSection } from './components/about-section';
import { ProductsSection } from './components/products-section';
import { ServicesSection } from './components/services-section';
import { PartnersSection } from './components/partners-section';
import { TestimonialsSection } from './components/testimonials-section';
import { ContactSection } from './components/contact-section';
import { Footer } from './components/footer';

@Component({
  selector: 'app-root',
  imports: [
    LoadingScreen, CustomCursor, Navbar, HeroSection, StatsSection,
    WaveSeparator, AboutSection, ProductsSection, ServicesSection,
    PartnersSection, TestimonialsSection, ContactSection, Footer,
  ],
  template: `
    <app-custom-cursor />

    @if (loading()) {
      <app-loading-screen (onDone)="handleLoadingDone()" />
    }

    <app-navbar />

    <main>
      <app-hero-section />
      <app-stats-section />

      <app-wave-separator fromColor="#071525" toColor="#F0FAFF" />
      <app-about-section />

      <app-wave-separator fromColor="#F0FAFF" toColor="#E8F7FF" />
      <app-products-section />

      <app-wave-separator fromColor="#E8F7FF" toColor="#F0FAFF" />
      <app-services-section />

      <app-wave-separator fromColor="#F0FAFF" toColor="#071525" />
      <app-partners-section />

      <app-wave-separator fromColor="#071525" toColor="#E8F7FF" />
      <app-testimonials-section />

      <app-wave-separator fromColor="#E8F7FF" toColor="#F0FAFF" />
      <app-contact-section />
    </main>

    <app-wave-separator fromColor="#F0FAFF" toColor="#071525" />
    <app-footer />

    <button (click)="scrollTop()"
      style="position: fixed; bottom: 2rem; left: 2rem; width: 48px; height: 48px; border-radius: 50%; background: linear-gradient(135deg, #2D9CDB, #1a7ab5); border: none; color: white; font-size: 1.3rem; cursor: none; display: flex; align-items: center; justify-content: center; box-shadow: 0 6px 24px rgba(45,156,219,0.45); z-index: 500; transition: transform 0.3s ease, box-shadow 0.3s ease;"
      (mouseenter)="onBtnHover($event)" (mouseleave)="onBtnLeave($event)"
      title="العودة للأعلى">↑</button>
  `,
})
export class App {
  protected readonly loading = signal(true);

  handleLoadingDone() {
    this.loading.set(false);
  }

  scrollTop() {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  onBtnHover(e: MouseEvent) {
    const el = e.currentTarget as HTMLElement;
    el.style.transform = 'translateY(-4px) scale(1.1)';
    el.style.boxShadow = '0 10px 35px rgba(45,156,219,0.6)';
  }

  onBtnLeave(e: MouseEvent) {
    const el = e.currentTarget as HTMLElement;
    el.style.transform = 'translateY(0) scale(1)';
    el.style.boxShadow = '0 6px 24px rgba(45,156,219,0.45)';
  }
}
