import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { PerformanceService, DashboardData } from '../performance.service';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
  public data: DashboardData | null = null;
  public loading: boolean = true;
  public errorMessage: string = '';

  constructor(private performanceService: PerformanceService) { }

 ngOnInit(): void {
  this.performanceService.getData().subscribe({
    next: (res: DashboardData) => {
      this.data = res;
      this.loading = false;
    },
    error: (err: any) => {
      this.errorMessage = 'שגיאה בטעינת קובץ הנתונים המקומי';
      this.loading = false;
      console.error('Error loading JSON:', err);
    }
  });
}

  getLowestTime(row: any): number {
    const times = [row.sqlTime, row.csharpTime, row.pythonTime].filter(
      (t) => t !== null && t !== undefined && !isNaN(t)
    );
    
    if (times.length === 0) return Infinity;
    
    return Math.min(...times);
  }
}