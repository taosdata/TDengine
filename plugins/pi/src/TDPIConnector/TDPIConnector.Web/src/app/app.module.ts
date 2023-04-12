import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { MonitoringComponent } from './monitoring/monitoring.component';
import { LogsComponent } from './log-page/logs.component';
import { HttpClientModule } from '@angular/common/http';
import { ApiHttpService } from './services/api-http-service';
import { LogContentComponent } from './log-content/log-content.component';
import { ExceptionsComponent } from './exceptions/exceptions.component';

@NgModule({
  declarations: [
    AppComponent,
    MonitoringComponent,
    LogsComponent,
    ExceptionsComponent,
    LogContentComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule 
  ],
  providers: [ApiHttpService],
  bootstrap: [AppComponent]
})
export class AppModule { }
