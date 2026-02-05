from django.urls import path
from main import views

urlpatterns = [
    path("", views.home, name="home"),
    path("api/dlt-errors/export-excel/", views.DltErrorToExcelAPIView.as_view()),
    path("api/iff/dlt-to-excel/", views.IffDltToExcelAPIView.as_view()),
    path("api/iff/excel-to-dlt/", views.ExcelToIffDltAPIView.as_view()),
    path("api/iff/commercial-manual-close/", views.ManuallyCloseCommercialDLT.as_view()),
]
