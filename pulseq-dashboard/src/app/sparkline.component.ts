import { Component, Input, OnChanges } from '@angular/core';

@Component({
  selector: 'app-sparkline',
  standalone: true,
  template: `
    <svg [attr.width]="width" [attr.height]="height" [attr.viewBox]="'0 0 ' + width + ' ' + height">
      <polyline [attr.points]="points" fill="none" stroke="currentColor" stroke-width="1.5"
                stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  `,
  styles: [':host { display: inline-block; vertical-align: middle; color: #22d3ee; }'],
})
export class SparklineComponent implements OnChanges {
  @Input() values: number[] = [];
  @Input() width = 80;
  @Input() height = 20;

  points = '';

  ngOnChanges(): void {
    const vals = this.values.slice(-40);
    if (vals.length < 2) {
      this.points = '';
      return;
    }
    const max = Math.max(...vals, 1);
    const step = this.width / (vals.length - 1);
    this.points = vals
      .map((v, i) => `${(i * step).toFixed(1)},${(this.height - (v / max) * this.height).toFixed(1)}`)
      .join(' ');
  }
}
