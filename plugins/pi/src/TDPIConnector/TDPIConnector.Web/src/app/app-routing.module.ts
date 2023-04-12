import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ExceptionsComponent } from './exceptions/exceptions.component';
import { LogContentComponent } from './log-content/log-content.component';
import { LogsComponent } from './log-page/logs.component';
import { MonitoringComponent } from './monitoring/monitoring.component';

const routes: Routes =[
  {path: 'exceptions', component: ExceptionsComponent},
  {path: 'monitoring', component: MonitoringComponent},
  {path: 'logs', component: LogsComponent},
  {path: 'log-content', component: LogContentComponent},
  {path: '', redirectTo: '/monitoring', pathMatch: 'full'},
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
