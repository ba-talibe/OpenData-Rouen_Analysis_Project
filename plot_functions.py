import plotly_express as px

def plot_comptage_horaire(df,name):
    data = df.loc[df['name']==name]
    return px.line(x=data['Time'],y=data['counts'])

def plot_comptage_jour(df,name):
    data = df.loc[df['name']==name,['Date','counts']]
    data = data.groupby('Date').sum()
    data = data.reset_index()
    return px.line(x=data['Date'],y=data['counts'])

def plot_comptage_mois(df,name):
    data = df.loc[df['name']==name,['Month-Year','counts']]
    data = data.groupby('Month-Year').sum()
    data = data.reset_index()
    return px.bar(x=data['Month-Year'],y=data['counts'])

def plot_comptage_annee(df,name):
    data = df.loc[df['name']==name,['Year','counts']]
    data = data.groupby('Year').sum()
    data = data.reset_index()
    return px.bar(x=data['Year'],y=data['counts'])

def plot_daily(df,name):
    data = df.loc[df['name']==name,['Hour','counts']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    return px.bar(x=data['Hour'],y=data['counts'])

def plot_daily_week_day_week_end(df,name):
    data = df.loc[(df['name']==name) & (df['DayOfWeek']<5),['Hour','counts']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    fig1 = px.bar(x=data['Hour'],y=data['counts'])
    data = df.loc[(df['name']==name) & (df['DayOfWeek']>=5),['Hour','counts']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    fig2 = px.bar(x=data['Hour'],y=data['counts'])
    return fig1, fig2
    
def plot_daily_by_day_of_week(df,name):
    l = []
    for dow in range(7):
        data = df.loc[(df['name']==name) & (df['DayOfWeek']==dow),['Hour','counts']]
        data = data.groupby('Hour').mean()
        data = data.reset_index()
        l.append(px.bar(x=data['Hour'],y=data['counts']))
    return tuple(l)

def plot_weekly(df,name):
    data = df.loc[df['name']==name,['DayOfWeek','counts']]
    data = data.groupby('DayOfWeek').mean()
    data = data.reset_index()
    return px.bar(x=data['DayOfWeek'],y=data['counts'])
