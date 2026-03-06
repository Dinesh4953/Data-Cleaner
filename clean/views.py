from django.shortcuts import render, redirect
import pandas as pd
from .models import DataRows
from django.contrib.auth.decorators import login_required
history_stack = []

import json
from django.core.serializers.json import DjangoJSONEncoder



from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import login

def register_view(request):
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("upload_dataset")
    else:
        form = UserCreationForm()

    return render(request, "clean/register.html", {"form": form})





@login_required
def upload_file(request):
     if request.method == "POST" and request.FILES.get("file"):
        DataRows.objects(user_id=request.user.id).delete()
        file = request.FILES["file"]
        df = pd.read_csv(file)
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = (df[col].astype("string").str.strip())
            
        
        metadata = {}
        for col in df.columns:
            unique_count = df[col].nunique(dropna=True)
            
            if pd.api.types.is_numeric_dtype(df[col]):
                metadata[col] = {
                    "type" : "numeric",
                    "unique" : []
                }
            else:
                if unique_count <= 20:
                    metadata[col] = {
                        "type" : "categorical",
                        "unique" : df[col].dropna().unique().tolist()
                    }
                else:
                    metadata[col] = {
                        "type" : "text",
                        "unique" : []
                    }
        request.session["metadata"] = json.dumps(metadata, cls=DjangoJSONEncoder)
        # Save CLEANED dataframe to DB
        records = df.to_dict(orient="records")

        chunk_size = 1000
        for start in range(0, len(records), chunk_size):
            chunk = records[start:start + chunk_size]
            for row in chunk:
                DataRows(
                    user_id=request.user.id,
                    data=row
                ).save()
            
        return redirect("table_view")
    
     return render(request, "clean/upload.html")


from django.core.paginator import Paginator

@login_required
def table_view(request):
   
    return render(request, "clean/table.html")



from django.http import JsonResponse
@login_required
def get_data(request):
    body = json.loads(request.body or "{}")
    mode = body.get("mode", "data")
    page = int(body.get("page", 1))
    filters = body.get("filters", [])
    page_size = body.get("page_size", 20)
    metadata = json.loads(request.session.get("metadata", "{}"))
    rows = list(DataRows.objects(user_id=request.user.id))
    
    for i,f in enumerate(filters):
        filtered_rows = []
        
        search_column = f.get("column")
        condition = f.get("condition")
        search_value = f.get("value")
        logic = f.get("logic", "AND")
        
        if not search_column or not search_value:
            continue
        column_type = metadata.get(search_column, {}).get("type")
        search_values = [v.strip().lower() for v in search_value.split(",")]
        
        for row in rows:
            if search_column not in row.data:
                continue
            
            value = row.data[search_column]
            
            match = False
            if column_type == "numeric":
                try:
                    value = float(value)
                except:
                    continue
                
                for val in search_values:
                    try:
                        if condition == "between":
                            parts = val.split("-")
                            if len(parts) != 2:
                                continue
                            
                            start = float(parts[0])
                            end = float(parts[1])
                            
                            if start < value < end :
                                match = True
                                break
                        else:
                            val = float(val)
                            if condition == "gt" and value > val:
                                match = True
                                break
                            elif condition == "lt" and value < val:
                                match = True
                                break
                            elif condition == "eq" and val == value:
                                match = True
                                break
                    except:
                        continue       
            else:
                value = str(value).strip().lower()
                
                if condition == "is":
                    match = value in search_values
                elif condition == "is_not" :
                    match = value not in search_values
                elif condition == "contains":
                    match = any(val in value for val in search_values)
                elif condition == "starts_with":
                    match = any(value.startswith(val) for val in search_values)
                elif condition == "end_with":
                    match = any(value.endswith(val) for val in search_values)
            
            if match:
                filtered_rows.append(row)
        
        if i == 0:
            rows = filtered_rows
        else:
            if logic == "AND":
                rows = [r for r in rows if r in filtered_rows]
            else:
                rows = list({*rows, *filtered_rows})
    if mode == "count":
        return JsonResponse({
            "total_count" : len(rows)
        })
        
    paginator = Paginator(rows, page_size)

    page_obj = paginator.get_page(page)

    data = [row.data for row in page_obj]

    columns = []
    if data:
        columns = list(data[0].keys())

    return JsonResponse({
        "data": data,
        "has_next": page_obj.has_next(),
        "has_previous": page_obj.has_previous(),
        "total_pages": paginator.num_pages,
        "current_page": page_obj.number,
        "columns": columns,
        "metadata": metadata
    })
                
            
            
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import pandas as pd
import json
import numpy as np
from .models import DataRows

@login_required
def clean_data(request):

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request"}, status=400)

    try:
        body = json.loads(request.body)
    except:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    column = body.get("column")
    method = body.get("method")
    value = body.get("value")

    if not method:
        return JsonResponse({"error": "Method required"}, status=400)

    rows = list(DataRows.objects(user_id=request.user.id))
    if not rows:
        return JsonResponse({"error": "No data available"}, status=400)

    df = pd.DataFrame([r.data for r in rows])

    # Save history for undo
    history = request.session.get("history", [])
    history.append(df.to_json())
    request.session["history"] = history
    request.session.modified = True

    # ================= DATASET LEVEL OPERATIONS =================

    if method == "remove_duplicates":

        if value:
            cols = [v.strip() for v in value.split(",")]
            valid_cols = [c for c in cols if c in df.columns]

            if not valid_cols:
                return JsonResponse({"error": "Invalid column names"}, status=400)

            df = df.drop_duplicates(subset=valid_cols, keep="first")
        else:
            df = df.drop_duplicates(keep="first")

    elif method == "remove_column":

        if not value:
            return JsonResponse({"error": "Provide column names"}, status=400)

        cols = [v.strip() for v in value.split(",")]
        valid_cols = [c for c in cols if c in df.columns]

        if not valid_cols:
            return JsonResponse({"error": "Invalid column names"}, status=400)

        df = df.drop(columns=valid_cols)

    elif method == "rename_column":

        if not value or ":" not in value:
            return JsonResponse({"error": "Use format old:new"}, status=400)

        old, new = value.split(":")
        old, new = old.strip(), new.strip()

        if old not in df.columns:
            return JsonResponse({"error": "Column not found"}, status=400)

        df = df.rename(columns={old: new})

    elif method == "remove_constant":

        constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
        df = df.drop(columns=constant_cols)

    elif method == "remove_high_missing":

        threshold = 0.5  # 50%
        missing_ratio = df.isnull().mean()
        cols_to_drop = missing_ratio[missing_ratio > threshold].index
        df = df.drop(columns=cols_to_drop)

    # ================= COLUMN LEVEL OPERATIONS =================
    else:

        if not column or column not in df.columns:
            return JsonResponse({"error": "Invalid column"}, status=400)

        metadata = json.loads(request.session.get("metadata", "{}"))
        column_type = metadata.get(column, {}).get("type")

        if column_type == "numeric":

            df[column] = pd.to_numeric(df[column], errors="coerce")

            if method == "fill_mean":
                df[column] = df[column].fillna(df[column].mean())

            elif method == "fill_median":
                df[column] = df[column].fillna(df[column].median())

            elif method == "fill_zero":
                df[column] = df[column].fillna(0)

            elif method == "replace":
                if not value or ":" not in value:
                    return JsonResponse({"error": "Use format old:new"}, status=400)

                old, new = value.split(":")
                df[column] = df[column].replace(float(old), float(new))

            elif method == "remove_outliers":
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                df = df[(df[column] >= lower) & (df[column] <= upper)]

            elif method == "cap_outliers":
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                df[column] = df[column].clip(lower, upper)

            elif method == "drop_na":
                df = df.dropna(subset=[column])

            elif method == "change_dtype":
                if value == "int":
                    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
                elif value == "float":
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                elif value == "datetime":
                    df[column] = pd.to_datetime(df[column], errors="coerce")

        else:

            df[column] = df[column].astype(str)

            if method == "fill_mode":
                mode_val = df[column].mode()
                if not mode_val.empty:
                    df[column] = df[column].fillna(mode_val[0])

            elif method == "lowercase":
                df[column] = df[column].str.lower()

            elif method == "uppercase":
                df[column] = df[column].str.upper()

            elif method == "titlecase":
                df[column] = df[column].str.title()

            elif method == "strip_spaces":
                df[column] = df[column].str.strip()

            elif method == "remove_special_chars":
                df[column] = df[column].str.replace(r"[^a-zA-Z0-9 ]", "", regex=True)

            elif method == "replace":
                if not value or ":" not in value:
                    return JsonResponse({"error": "Use format old:new"}, status=400)

                old, new = value.split(":")
                df[column] = df[column].replace(old, new)

            elif method == "drop_na":
                df = df.dropna(subset=[column])

    # ================= SAVE TO DATABASE =================

    df = df.replace({np.nan: None}).reset_index(drop=True)
    records = df.to_dict(orient="records")

    DataRows.objects(user_id=request.user.id).delete()

    chunk_size = 1000
    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]
        for row in chunk:
            DataRows(
                user_id=request.user.id,
                data=row
            ).save()

    return JsonResponse({
        "status": "success",
        "message": f"{method} applied successfully"
    })
    
@login_required
def undo_cleaning(request):
    history = request.session.get("history", [])

    if not history:
        return JsonResponse({"status": "no_history"})

    last_json = history.pop()
    request.session["history"] = history
    import io

    df = pd.read_json(io.StringIO(last_json))

    # Reset index
    df = df.reset_index(drop=True)

    # Delete current DB rows
    DataRows.objects(user_id=request.user.id).delete()

    for _, row in df.iterrows():
        DataRows(
            user_id=request.user.id,
            data=row.to_dict()
        ).save()

    return JsonResponse({"status": "success"})


@login_required
def dataset_info(request):

    rows = list(DataRows.objects(user_id=request.user.id))
    if not rows:
        return JsonResponse({"error": "No data"})

    df = pd.DataFrame([r.data for r in rows])

    shape = df.shape
    columns = list(df.columns)
    dtypes = df.dtypes.astype(str).to_dict()
    missing = df.isnull().sum().to_dict()

    # Numeric describe
    numeric_df = df.select_dtypes(include=["number"])
    if not numeric_df.empty:
        describe_numeric = numeric_df.describe().fillna("").to_dict()
    else:
        describe_numeric = {}

    # Categorical describe
    categorical_df = df.select_dtypes(include=["object"])
    if not categorical_df.empty:
        describe_categorical = categorical_df.describe().fillna("").to_dict()
    else:
        describe_categorical = {}

    return JsonResponse({
        "shape": shape,
        "columns": columns,
        "dtypes": dtypes,
        "missing": missing,
        "describe_numeric": describe_numeric,
        "describe_categorical": describe_categorical
    })


import numpy as np
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE


@login_required
def preprocess_data(request):

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request"}, status=400)

    body = json.loads(request.body)

    operation = body.get("operation")
    column = body.get("column")
    target = body.get("target")

    rows = list(DataRows.objects(user_id=request.user.id))
    if not rows:
        return JsonResponse({"error": "No data available"}, status=400)

    df = pd.DataFrame([r.data for r in rows])

    # Save history for undo
    history = request.session.get("history", [])
    history.append(df.to_json())
    request.session["history"] = history
    request.session.modified = True

    # ================= SCALING =================
    if operation == "standardize":

        df[column] = pd.to_numeric(df[column], errors="coerce")

        std = df[column].std()
        if std != 0:
            df[column] = (df[column] - df[column].mean()) / std

    elif operation == "normalize":

        df[column] = pd.to_numeric(df[column], errors="coerce")

        min_val = df[column].min()
        max_val = df[column].max()

        if max_val != min_val:
            df[column] = (df[column] - min_val) / (max_val - min_val)
    # ================= ENCODING =================
    elif operation == "label_encode":
        
        le = LabelEncoder()
        df[column] = le.fit_transform(df[column].astype(str))

    # ================= HANDLE IMBALANCE =================
    elif operation == "smote":

        if not target:
            return JsonResponse({"error": "Target column required"}, status=400)

        

        # Clean string columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()

        X = df.drop(columns=[target])
        y = df[target]

        # If target is categorical text → encode it
        if y.dtype == "object" or str(y.dtype).startswith("category"):
            le = LabelEncoder()
            y = le.fit_transform(y)

        # Validate target
        if len(set(y)) < 2:
            return JsonResponse({"error": "Target must have at least 2 classes"}, status=400)

        class_counts = pd.Series(y).value_counts()
        if class_counts.min() == class_counts.max():
            return JsonResponse({"error": "Dataset already balanced"}, status=400)

        # Convert categorical feature columns
        X = pd.get_dummies(X, drop_first=True)

        sm = SMOTE()
        X_res, y_res = sm.fit_resample(X, y)

        df = pd.concat([
            pd.DataFrame(X_res, columns=X.columns),
            pd.DataFrame(y_res, columns=[target])
        ], axis=1)
    else:
        return JsonResponse({"error": "Invalid operation"}, status=400)

    # Reset index

# Convert NaN to None (JSON safe)
    df = df.replace({np.nan: None})

    records = df.to_dict(orient="records")

    # Convert numpy values to native Python
    for row in records:
        for k, v in row.items():
            if isinstance(v, np.generic):
                row[k] = v.item()

    # Reset index BEFORE saving
    df = pd.DataFrame(records).reset_index(drop=True)
    records = df.to_dict(orient="records")

    # Replace database data (ONLY ONCE)
    DataRows.objects(user_id=request.user.id).delete()

    for row in records:
        DataRows(
            user_id=request.user.id,
            data=row
        ).save()

    return JsonResponse({"status": "success"})



@login_required
def group_data(request):

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request"}, status=400)

    body = json.loads(request.body)
    column = body.get("column")
    group_value = body.get("group_value")

    rows = list(DataRows.objects(user_id=request.user.id))
    if not rows:
        return JsonResponse({"error": "No data available"}, status=400)

    df = pd.DataFrame([r.data for r in rows])

    if column not in df.columns:
        return JsonResponse({"error": "Invalid column"}, status=400)

    # If group_value NOT provided → return grouped summary
    if not group_value:

        grouped = df.groupby(column)

        result = []
        for name, group_df in grouped:
            result.append({
                "name": str(name),
                "count": len(group_df)
            })

        return JsonResponse({
            "column": column,
            "groups": result
        })

    # If group_value provided → return rows of that group
    else:

        filtered_df = df[
    df[column].astype(str).str.strip().str.lower() 
    ==  str(group_value).strip().lower()
]

        return JsonResponse({
            "data": filtered_df.to_dict(orient="records"),
            "columns": list(filtered_df.columns),
            "current_page": 1,
            "total_pages": 1
        })
        
        
        
import plotly.express as px
import json

@login_required
def visualize_data(request):

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request"}, status=400)

    body = json.loads(request.body)
    columns = body.get("columns")
    chart_type = body.get("chart_type")

    rows = list(DataRows.objects(user_id=request.user.id))
    df = pd.DataFrame([r.data for r in rows])
    # df = df.apply(pd.to_numeric, errors="ignore")

    # Convert numeric columns safely
    for col in columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass

    template_style = "plotly"

    if chart_type == "histogram":
        fig = px.histogram(df, x=columns[0], template=template_style)

    elif chart_type == "boxplot":
        fig = px.box(df, y=columns[0], template=template_style)

    elif chart_type == "violin":
        fig = px.violin(df, y=columns[0], box=True, points="all", template=template_style)

    elif chart_type == "scatter":
        fig = px.scatter(df, x=columns[0], y=columns[1], template=template_style)

    elif chart_type == "line":
        fig = px.line(df, x=columns[0], y=columns[1], template=template_style)

    elif chart_type == "bar":
        fig = px.bar(df, x=columns[0], template=template_style)

    elif chart_type == "pie":
        fig = px.pie(df, names=columns[0], template=template_style)
        
    elif chart_type == "donut":
        fig = px.pie(
            df,
            names=columns[0],
            hole=0.4, 
            template=template_style
        )
    elif chart_type == "area":
        df = df.dropna(subset=columns)
        fig = px.area(
            df,
            x=columns[0],
            y=columns[1],
            template=template_style
        )
    elif chart_type == "heatmap":
        df = df.dropna(subset=columns)
        fig = px.density_heatmap(
            df,
            x=columns[0],
            y=columns[1],
            template=template_style
        )
    elif chart_type == "bubble":
        df = df.dropna(subset=columns)
        fig = px.scatter(
            df,
            x=columns[0],
            y=columns[1],
            size=columns[2],
            template=template_style
    )
    elif chart_type == "3d":
        df = df.dropna(subset=columns)
        fig = px.scatter_3d(
            df,
            x=columns[0],
            y=columns[1],
            z=columns[2],
            template=template_style
    )
    
    elif chart_type == "parallel":
        df = df.dropna(subset=columns)
        fig = px.parallel_coordinates(
            df,
            dimensions=columns,
            template=template_style
        )
    elif chart_type == "pairplot":

    # Select only numeric columns
        numeric_df = df.select_dtypes(include=["number"])

        if numeric_df.empty:
            return JsonResponse({"error": "No numeric columns available"}, status=400)

        fig = px.scatter_matrix(
            numeric_df,
            dimensions=numeric_df.columns,
            template=template_style
        )

    else:
        return JsonResponse({"error": "Invalid chart type"}, status=400)

    if chart_type == "pairplot":
        chart_height = 1200
    else:
        chart_height = 750

    fig.update_layout(
        height=chart_height,
        barmode="group",
        template=template_style
    )

    graph_json = json.loads(fig.to_json())
    return JsonResponse(graph_json)
        
