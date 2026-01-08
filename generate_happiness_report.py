# generate_happiness_report.py
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from config import PG_CONFIG
import os
from datetime import datetime
import numpy as np


class HappinessReportGenerator:
    """Generate comprehensive happiness vs temperature analysis reports."""
    
    def __init__(self, output_dir="reports"):
        self.output_dir = output_dir
        self.df = None
        os.makedirs(output_dir, exist_ok=True)
    
    def fetch_data(self):
        """Fetch happiness and weather data from database."""
        query = """
        SELECT 
            dc.ladder_score AS happiness_score, 
            fw.temperature_celsius, 
            dc.country_name,
            dcity.city_name,
            fw.fetch_date,
            dc.gdp_per_capita,
            dc.social_support,
            dc.healthy_life_expectancy
        FROM dim_country dc
        LEFT JOIN dim_city dcity ON dc.country_name = dcity.country_name
        LEFT JOIN fact_weather_snapshot fw ON dcity.city_id = fw.city_id
        WHERE fw.fetch_date::date = CURRENT_DATE
          AND fw.temperature_celsius IS NOT NULL
          AND dc.ladder_score IS NOT NULL
        ORDER BY dc.ladder_score DESC
        """
        
        try:
            with psycopg2.connect(**PG_CONFIG) as conn:
                self.df = pd.read_sql(query, conn)
                
            if self.df.empty:
                raise ValueError("No data retrieved. Ensure ETL has run and weather data exists for today.")
            
            print(f"✓ Loaded {len(self.df)} records from {self.df['country_name'].nunique()} countries")
            return self.df
            
        except psycopg2.Error as e:
            raise ConnectionError(f"Database error: {e}")
    
    def calculate_statistics(self):
        """Calculate correlation and statistical metrics."""
        if self.df is None or self.df.empty:
            return None
        
        correlation, p_value = stats.pearsonr(
            self.df['temperature_celsius'], 
            self.df['happiness_score']
        )
        
        stats_summary = {
            'correlation': correlation,
            'p_value': p_value,
            'mean_happiness': self.df['happiness_score'].mean(),
            'mean_temp': self.df['temperature_celsius'].mean(),
            'total_cities': len(self.df),
            'total_countries': self.df['country_name'].nunique()
        }
        
        print(f"\n📊 Statistical Analysis:")
        print(f"   Correlation coefficient: {correlation:.3f}")
        print(f"   P-value: {p_value:.4f}")
        print(f"   Relationship: {'Significant' if p_value < 0.05 else 'Not significant'}")
        
        return stats_summary
    
    def create_main_visualization(self, stats_summary):
        """Create enhanced scatter plot with regression line."""
        fig, ax = plt.subplots(figsize=(16, 10))
        sns.set_style("whitegrid")
        
        # Main scatter plot
        scatter = ax.scatter(
            self.df['temperature_celsius'],
            self.df['happiness_score'],
            c=self.df['happiness_score'],
            cmap='RdYlGn',
            s=150,
            alpha=0.7,
            edgecolors='black',
            linewidth=0.8
        )
        
        # Add regression line
        z = np.polyfit(self.df['temperature_celsius'], self.df['happiness_score'], 1)
        p = np.poly1d(z)
        temp_range = np.linspace(self.df['temperature_celsius'].min(), 
                                self.df['temperature_celsius'].max(), 100)
        ax.plot(temp_range, p(temp_range), "r--", alpha=0.6, linewidth=2, 
                label=f'Trend line (r={stats_summary["correlation"]:.3f})')
        
        # Highlight top 5 happiest countries
        top5 = self.df.nlargest(5, 'happiness_score')
        for _, row in top5.iterrows():
            ax.annotate(
                f"{row['city_name']}\n({row['happiness_score']:.2f})",
                xy=(row['temperature_celsius'], row['happiness_score']),
                xytext=(10, 10),
                textcoords='offset points',
                fontsize=9,
                fontweight='bold',
                color='darkgreen',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.7),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', color='darkgreen')
            )
        
        # Highlight Kenya cities
        kenyan = self.df[self.df['country_name'] == 'Kenya']
        if not kenyan.empty:
            ax.scatter(
                kenyan['temperature_celsius'],
                kenyan['happiness_score'],
                s=250,
                marker='*',
                color='red',
                edgecolors='darkred',
                linewidth=2,
                label=f'Kenya cities (n={len(kenyan)})',
                zorder=5
            )
            for _, row in kenyan.iterrows():
                ax.annotate(
                    row['city_name'],
                    xy=(row['temperature_celsius'], row['happiness_score']),
                    xytext=(-15, -25),
                    textcoords='offset points',
                    fontsize=9,
                    fontweight='bold',
                    color='darkred',
                    bbox=dict(boxstyle='round,pad=0.4', facecolor='lightcoral', alpha=0.8)
                )
        
        # Styling
        ax.set_title(
            f"Global Happiness vs Current Temperature\n"
            f"Live Data Analysis — {datetime.now().strftime('%B %d, %Y')}\n"
            f"Cities: {stats_summary['total_cities']} | Countries: {stats_summary['total_countries']}",
            fontsize=18,
            fontweight='bold',
            pad=20
        )
        ax.set_xlabel("Current Temperature (°C)", fontsize=14, fontweight='bold')
        ax.set_ylabel("Happiness Score (2024 World Happiness Report)", fontsize=14, fontweight='bold')
        ax.legend(loc='lower right', fontsize=11)
        ax.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Happiness Score', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        output_path = os.path.join(self.output_dir, 'happiness_vs_temperature_global.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"\n✓ Main visualization saved: {os.path.abspath(output_path)}")
        
        return output_path
    
    def create_distribution_plots(self):
        """Create distribution analysis subplots."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Temperature distribution
        axes[0, 0].hist(self.df['temperature_celsius'], bins=30, color='skyblue', 
                       edgecolor='black', alpha=0.7)
        axes[0, 0].axvline(self.df['temperature_celsius'].mean(), color='red', 
                          linestyle='--', linewidth=2, label=f"Mean: {self.df['temperature_celsius'].mean():.1f}°C")
        axes[0, 0].axvline(self.df['temperature_celsius'].median(), color='orange', 
                          linestyle=':', linewidth=2, label=f"Median: {self.df['temperature_celsius'].median():.1f}°C")
        axes[0, 0].set_title('Temperature Distribution', fontsize=14, fontweight='bold')
        axes[0, 0].set_xlabel('Temperature (°C)')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].legend()
        
        # Happiness distribution
        axes[0, 1].hist(self.df['happiness_score'], bins=30, color='lightgreen', 
                       edgecolor='black', alpha=0.7)
        axes[0, 1].axvline(self.df['happiness_score'].mean(), color='red', 
                          linestyle='--', linewidth=2, label=f"Mean: {self.df['happiness_score'].mean():.2f}")
        axes[0, 1].axvline(self.df['happiness_score'].median(), color='orange', 
                          linestyle=':', linewidth=2, label=f"Median: {self.df['happiness_score'].median():.2f}")
        axes[0, 1].set_title('Happiness Score Distribution', fontsize=14, fontweight='bold')
        axes[0, 1].set_xlabel('Happiness Score')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].legend()
        
        # Temperature ranges vs happiness
        temp_bins = pd.cut(self.df['temperature_celsius'], bins=5)
        temp_happiness = self.df.groupby(temp_bins, observed=True)['happiness_score'].mean()
        colors = plt.cm.coolwarm(np.linspace(0, 1, len(temp_happiness)))
        axes[1, 0].bar(range(len(temp_happiness)), temp_happiness.values, 
                      color=colors, edgecolor='black', alpha=0.7)
        axes[1, 0].set_xticks(range(len(temp_happiness)))
        axes[1, 0].set_xticklabels([f"{interval.left:.0f}-{interval.right:.0f}°C" 
                                    for interval in temp_happiness.index], rotation=45, ha='right')
        axes[1, 0].set_title('Average Happiness by Temperature Range', fontsize=14, fontweight='bold')
        axes[1, 0].set_ylabel('Average Happiness Score')
        axes[1, 0].set_ylim(temp_happiness.min() * 0.95, temp_happiness.max() * 1.05)
        
        # Top 10 happiest cities
        top10 = self.df.nlargest(10, 'happiness_score')
        colors = plt.cm.YlGn(np.linspace(0.4, 0.9, len(top10)))
        axes[1, 1].barh(range(len(top10)), top10['happiness_score'].values, 
                       color=colors, edgecolor='black', alpha=0.8)
        axes[1, 1].set_yticks(range(len(top10)))
        axes[1, 1].set_yticklabels([f"{row['city_name']} ({row['temperature_celsius']:.1f}°C)" 
                                    for _, row in top10.iterrows()], fontsize=10)
        axes[1, 1].set_title('Top 10 Happiest Cities', fontsize=14, fontweight='bold')
        axes[1, 1].set_xlabel('Happiness Score')
        axes[1, 1].invert_yaxis()
        
        # Add values on bars
        for i, v in enumerate(top10['happiness_score'].values):
            axes[1, 1].text(v + 0.05, i, f'{v:.2f}', va='center', fontweight='bold')
        
        plt.tight_layout()
        output_path = os.path.join(self.output_dir, 'happiness_distributions.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✓ Distribution analysis saved: {os.path.abspath(output_path)}")
        
        return output_path
    
    def generate_insights(self):
        """Generate text-based insights from the data."""
        insights = []
        
        # Overall statistics
        insights.append(f"📈 Overall Statistics:")
        insights.append(f"   • Average global happiness: {self.df['happiness_score'].mean():.2f} (±{self.df['happiness_score'].std():.2f})")
        insights.append(f"   • Average temperature: {self.df['temperature_celsius'].mean():.1f}°C (±{self.df['temperature_celsius'].std():.1f}°C)")
        insights.append(f"   • Happiness range: {self.df['happiness_score'].min():.2f} - {self.df['happiness_score'].max():.2f}")
        insights.append(f"   • Temperature range: {self.df['temperature_celsius'].min():.1f}°C - {self.df['temperature_celsius'].max():.1f}°C")
        
        # Temperature extremes
        coldest = self.df.loc[self.df['temperature_celsius'].idxmin()]
        warmest = self.df.loc[self.df['temperature_celsius'].idxmax()]
        insights.append(f"\n🌡️ Temperature Extremes:")
        insights.append(f"   • Coldest: {coldest['city_name']}, {coldest['country_name']} ({coldest['temperature_celsius']:.1f}°C, happiness: {coldest['happiness_score']:.2f})")
        insights.append(f"   • Warmest: {warmest['city_name']}, {warmest['country_name']} ({warmest['temperature_celsius']:.1f}°C, happiness: {warmest['happiness_score']:.2f})")
        
        # Happiness extremes
        happiest = self.df.loc[self.df['happiness_score'].idxmax()]
        least_happy = self.df.loc[self.df['happiness_score'].idxmin()]
        insights.append(f"\n😊 Happiness Extremes:")
        insights.append(f"   • Happiest: {happiest['city_name']}, {happiest['country_name']} (Score: {happiest['happiness_score']:.2f}, Temp: {happiest['temperature_celsius']:.1f}°C)")
        insights.append(f"   • Least happy: {least_happy['city_name']}, {least_happy['country_name']} (Score: {least_happy['happiness_score']:.2f}, Temp: {least_happy['temperature_celsius']:.1f}°C)")
        
        # Temperature-Happiness segments
        cold_cities = self.df[self.df['temperature_celsius'] < 10]
        warm_cities = self.df[self.df['temperature_celsius'] > 25]
        if not cold_cities.empty and not warm_cities.empty:
            insights.append(f"\n❄️🌞 Climate Comparison:")
            insights.append(f"   • Cold cities (<10°C): Avg happiness {cold_cities['happiness_score'].mean():.2f} ({len(cold_cities)} cities)")
            insights.append(f"   • Warm cities (>25°C): Avg happiness {warm_cities['happiness_score'].mean():.2f} ({len(warm_cities)} cities)")
        
        # Kenya analysis
        kenyan = self.df[self.df['country_name'] == 'Kenya']
        if not kenyan.empty:
            global_avg = self.df['happiness_score'].mean()
            kenya_avg = kenyan['happiness_score'].mean()
            diff = kenya_avg - global_avg
            insights.append(f"\n🇰🇪 Kenya Deep Dive:")
            insights.append(f"   • Cities tracked: {len(kenyan)}")
            insights.append(f"   • Average happiness: {kenya_avg:.2f} ({'↑' if diff > 0 else '↓'}{abs(diff):.2f} vs global avg)")
            insights.append(f"   • Average temperature: {kenyan['temperature_celsius'].mean():.1f}°C")
            insights.append(f"   • Temperature range: {kenyan['temperature_celsius'].min():.1f}°C - {kenyan['temperature_celsius'].max():.1f}°C")
            insights.append(f"   • Cities: {', '.join(kenyan['city_name'].tolist())}")
        
        # Top performers
        top5 = self.df.nlargest(5, 'happiness_score')
        insights.append(f"\n🏆 Top 5 Happiest Cities:")
        for i, (_, row) in enumerate(top5.iterrows(), 1):
            insights.append(f"   {i}. {row['city_name']}, {row['country_name']} - {row['happiness_score']:.2f} (Temp: {row['temperature_celsius']:.1f}°C)")
        
        # Regional analysis
        if 'latitude' in self.df.columns and self.df['latitude'].notna().any():
            self.df['hemisphere'] = self.df['latitude'].apply(lambda x: 'Northern' if x > 0 else 'Southern')
            hemisphere_stats = self.df.groupby('hemisphere')['happiness_score'].agg(['mean', 'count'])
            insights.append(f"\n🌍 Hemisphere Analysis:")
            for hemisphere, stats in hemisphere_stats.iterrows():
                insights.append(f"   • {hemisphere}: Avg happiness {stats['mean']:.2f} ({int(stats['count'])} cities)")
        
        return "\n".join(insights)
    
    def generate_report(self):
        """Main method to generate complete report."""
        try:
            print("=" * 70)
            print("🌍 GLOBAL HAPPINESS vs TEMPERATURE REPORT GENERATOR")
            print("=" * 70)
            
            # Fetch data
            self.fetch_data()
            
            # Calculate statistics
            stats_summary = self.calculate_statistics()
            
            # Create visualizations
            self.create_main_visualization(stats_summary)
            self.create_distribution_plots()
            
            # Generate and save insights
            insights = self.generate_insights()
            print("\n" + "=" * 70)
            print("📝 KEY INSIGHTS")
            print("=" * 70)
            print(insights)
            
            # Save insights to text file
            insights_path = os.path.join(self.output_dir, 'report_insights.txt')
            with open(insights_path, 'w', encoding='utf-8') as f:
                f.write("GLOBAL HAPPINESS vs TEMPERATURE ANALYSIS\n")
                f.write(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}\n")
                f.write("=" * 70 + "\n\n")
                f.write(insights)
                f.write("\n\n" + "=" * 70 + "\n")
                f.write(f"Statistical Summary:\n")
                f.write(f"  • Correlation: {stats_summary['correlation']:.4f}\n")
                f.write(f"  • P-value: {stats_summary['p_value']:.6f}\n")
                f.write(f"  • Cities analyzed: {stats_summary['total_cities']}\n")
                f.write(f"  • Countries covered: {stats_summary['total_countries']}\n")
            
            print(f"\n✓ Insights saved: {os.path.abspath(insights_path)}")
            
            print("\n" + "=" * 70)
            print("✅ REPORT GENERATION COMPLETE")
            print("=" * 70)
            print(f"📁 Output directory: {os.path.abspath(self.output_dir)}")
            print(f"📊 Files generated:")
            print(f"   • happiness_vs_temperature_global.png")
            print(f"   • happiness_distributions.png")
            print(f"   • report_insights.txt")
            
        except Exception as e:
            print(f"\n❌ Error generating report: {e}")
            import traceback
            traceback.print_exc()
            raise


def main():
    """Entry point for the script."""
    generator = HappinessReportGenerator()
    generator.generate_report()


if __name__ == "__main__":
    main()