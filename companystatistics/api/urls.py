from django.urls import path
from . import views
urlpatterns=[
    # localhost/api/corps
    path("corps",views.fetch_comp_data,name="fetch_comp_data"),
    # localhost/api/esgscore/<riccode>
    path("esgscore/<str:riccode>/",views.fetch_esg_scores),
]