import plotly.express as px

def plot_comptage_horaire(df,name):
    data = df.loc[df['Nom']==name]
    return px.line(x=data['Time'],y=data['Comptage'])

def plot_comptage_jour(df,name):
    data = df.loc[df['Nom']==name,['Date','Comptage']]
    data = data.groupby('Date').sum()
    data = data.reset_index()
    return px.line(x=data['Date'],y=data['Comptage'])

def plot_comptage_mois(df,name):
    data = df.loc[df['Nom']==name,['Month-Year','Comptage']]
    data = data.groupby('Month-Year').sum()
    data = data.reset_index()
    return px.bar(x=data['Month-Year'],y=data['Comptage'])

def plot_comptage_annee(df,name):
    data = df.loc[df['Nom']==name,['Year','Comptage']]
    data = data.groupby('Year').sum()
    data = data.reset_index()
    return px.bar(x=data['Year'],y=data['Comptage'])

def plot_daily(df,name):
    data = df.loc[df['Nom']==name,['Hour','Comptage']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    return px.bar(x=data['Hour'],y=data['Comptage'])

def plot_daily_week_day_week_end(df,name):
    data = df.loc[(df['Nom']==name) & (df['DayOfWeek']<5),['Hour','Comptage']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    fig1 = px.bar(x=data['Hour'],y=data['Comptage'])
    data = df.loc[(df['Nom']==name) & (df['DayOfWeek']>=5),['Hour','Comptage']]
    data = data.groupby('Hour').mean()
    data = data.reset_index()
    fig2 = px.bar(x=data['Hour'],y=data['Comptage'])
    return fig1, fig2
    
def plot_daily_by_day_of_week(df,name):
    l = []
    for dow in range(7):
        data = df.loc[(df['Nom']==name) & (df['DayOfWeek']==dow),['Hour','Comptage']]
        data = data.groupby('Hour').mean()
        data = data.reset_index()
        l.append(px.bar(x=data['Hour'],y=data['Comptage']))
    return tuple(l)

def plot_weekly(df,name):
    data = df.loc[df['Nom']==name,['DayOfWeek','Comptage']]
    data = data.groupby('DayOfWeek').mean()
    data = data.reset_index()
    return px.bar(x=data['DayOfWeek'],y=data['Comptage'])
