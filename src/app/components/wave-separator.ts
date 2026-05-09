import { Component, input } from '@angular/core';

@Component({
  selector: 'app-wave-separator',
  template: `
    <div class="wave-separator" [style.background]="fromColor()" [style.transform]="flip() ? 'scaleY(-1)' : 'none'">
      <svg viewBox="0 0 1440 70" preserveAspectRatio="none" style="width: 100%; height: 70px; display: block;">
        <path [attr.d]="'M0,35 C240,70 480,0 720,35 C960,70 1200,10 1440,35 L1440,70 L0,70 Z'" [attr.fill]="toColor()" opacity="0.5" />
        <path [attr.d]="'M0,50 C360,15 720,65 1080,40 C1260,28 1380,38 1440,50 L1440,70 L0,70 Z'" [attr.fill]="toColor()" />
      </svg>
    </div>
  `,
  standalone: true,
})
export class WaveSeparator {
  readonly fromColor = input('#F0FAFF');
  readonly toColor = input('#071525');
  readonly flip = input(false);
}
