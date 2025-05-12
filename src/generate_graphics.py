from src.dlt_pipeline.db_access import Database
import matplotlib.pyplot as plt
from pathlib import Path

name = 'maine_legislation_and_testimony'
db = Database(name)
fig_path = Path(__file__).parent.parent / 'figs'

def plot_testimony_volume():
    query = '''
        WITH TESTIMONIES AS (
            SELECT
                COUNT(DISTINCT Id) AS TESTIMONIES,
                legislature AS LEGISLATURE
            FROM TESTIMONY_HEADER
            GROUP BY legislature
        )
        , BILLS AS (
            SELECT
                COUNT(DISTINCT ldNumber) AS BILLS,
                legislature AS LEGISLATURE
            FROM BILL_TEXT
            GROUP BY legislature
        )
        SELECT 
            TESTIMONIES,
            BILLS,
            b.LEGISLATURE
        FROM BILLS b
        JOIN TESTIMONIES t
        ON b.LEGISLATURE = t.LEGISLATURE
    '''

    df = db.return_query_as_df(query)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    x = df['LEGISLATURE'].sort_values()
    ax1.bar(x, df['TESTIMONIES'], color='skyblue')
    ax2 = ax1.twinx()
    ax2.plot(x, df['BILLS'], color='red', marker='o')
    ax2.set_ylim(0, None)
    ax1.set_ylabel('Count of testimonies', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax2.set_ylabel('Count of bills', color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    plt.xlabel('Legislature')
    plt.title('All testimony and bills by session')
    fig_name = 'all_testimony_trend.png'
    save_path = fig_path / fig_name
    plt.savefig(save_path, dpi=300, bbox_inches='tight')

def plot_sierra_trend():
    query = '''
        SELECT 
            COUNT(DISTINCT Id) AS TESTIMONIES,
            legislature AS LEGISLATURE,
            GROUP_CONCAT(DISTINCT Organization) AS ORG_NAMES
        FROM TESTIMONY_HEADER th 
        WHERE Organization LIKE '%Sierra%'
        GROUP BY legislature
    '''
    df = db.return_query_as_df(query)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    x = df['LEGISLATURE'].sort_values()
    ax1.bar(x, df['TESTIMONIES'], color='skyblue')
    ax1.set_ylabel('Sierra Club Testimony', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    plt.xlabel('Legislature')
    plt.title('Sierra Club testimony and bills by session')
    fig_name = 'sierra_testimony_trend.png'
    save_path = fig_path / fig_name
    plt.savefig(save_path, dpi=300, bbox_inches='tight')

def plot_policy_area():
    query = '''
            SELECT 
                COUNT(DISTINCT Id) AS TESTIMONIES,
                PolicyArea AS POLICY_AREA,
                GROUP_CONCAT(DISTINCT Organization) AS ORG_NAMES
            FROM TESTIMONY_HEADER th 
            WHERE Organization LIKE '%Sierra%'
            GROUP BY PolicyArea
    '''
    df = db.return_query_as_df(query)
    fig, ax1 = plt.subplots(figsize=(10, 6))
    sorted_df = df.sort_values(by='TESTIMONIES', ascending=True)
    ax1.barh(sorted_df['POLICY_AREA'], sorted_df['TESTIMONIES'], color='skyblue')
    ax1.set_xlabel('Sierra Club Testimony', color='blue')
    ax1.tick_params(axis='x', labelcolor='blue')
    plt.ylabel('Policy Area')
    plt.title('Sierra Club Testimony by Policy Area')

    fig_name = 'sierra_testimony_by_topic.png'
    save_path = fig_path / fig_name
    plt.savefig(save_path, dpi=300, bbox_inches='tight')

    return df


if __name__ == '__main__':
    plot_testimony_volume()
    plot_sierra_trend()
    plot_policy_area()