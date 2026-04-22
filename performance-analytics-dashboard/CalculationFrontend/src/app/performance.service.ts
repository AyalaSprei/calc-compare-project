import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';

export interface TableRow {
  targil_id: number;
  sqlTime: number;
  csharpTime: number;
  pythonTime: number;
}

export interface OverallAverages {
  avgSql: number;
  avgCsharp: number;
  avgPython: number;
}

export interface DashboardData {
  mismatchedCount: number;
  tableDetails: TableRow[];
  overallAverages: OverallAverages;
}

@Injectable({
  providedIn: 'root'
})
export class PerformanceService {
  private readonly jsonPath = 'assets/data.json';

  constructor(private http: HttpClient) { }

  getData(): Observable<DashboardData> {
    return this.http.get<any>(this.jsonPath).pipe(
      map(res => {
        const parsedAverages = typeof res.overallAverages === 'string' 
          ? JSON.parse(res.overallAverages) 
          : res.overallAverages;

        return {
          mismatchedCount: res.mismatchedCount || 0, 
          tableDetails: res.tableDetails,
          overallAverages: parsedAverages
        } as DashboardData;
      })
    );
  }
}