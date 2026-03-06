from django.urls import path
from . import views
from django.contrib.auth import views as auth_views
from . import views

urlpatterns = [
  path("", auth_views.LoginView.as_view(template_name="clean/login.html"), name="login"),
      path("upload/", views.upload_file, name="upload_dataset"),
    path("table/", views.table_view, name="table_view"),
    path("get_data/", views.get_data, name="get_data"),
    path("clean_data/", views.clean_data, name="clean_data"),
    path("undo_cleaning/", views.undo_cleaning, name="undo_cleaning"),
    path('dataset_info/', views.dataset_info, name='dataset_info'),
    path("preprocess_data/", views.preprocess_data, name="preprocess_data"),
    path("group_data/", views.group_data, name="group_data"),
    path("visualize_data/", views.visualize_data, name="visualize_data"),
    path("register/", views.register_view, name="register"),
    path("logout/", auth_views.LogoutView.as_view(), name="logout"),
    


]

# urlpatterns = [
#     path("", views.upload_file, name="upload_file"),
#     path('table/', views.table_view, name='table_view'),
# ]
