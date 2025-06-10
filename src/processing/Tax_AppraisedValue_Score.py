import locale # For currency formatting (optional but nice)

# Optional: Set locale for currency formatting (adjust if needed for your system)
try:
    # Use a common US locale
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except locale.Error:
    print("Warning: Locale 'en_US.UTF-8' not found. Using default locale for formatting.")
    # locale.setlocale(locale.LC_ALL, '') # Fallback to system default

def clean_numeric_value(value):
    """
    Cleans a numeric value that may contain formatting characters like $ and commas.
    
    Args:
        value: The value to clean (string, int, float, etc.)
        
    Returns:
        float: The cleaned value as a float
        
    Raises:
        ValueError: If the value cannot be converted to a float after cleaning
    """
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Check for unknown
        if value.lower() == "unknown":
            return "Unknown"
            
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        
        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            raise ValueError(f"Could not convert string to float: '{value}'")
    
    # For other types, try simple conversion
    return float(value)

class DelinquencyDataPoint:
    """
    Represents a data point comparing a property's appraised value
    to the amount of delinquent taxes owed.

    Calculates the delinquency ratio (tax_amount / appraised_value).
    """
    def __init__(self, appraised_value, tax_amount):
        """
        Initializes the data point.

        Args:
            appraised_value (float | int | str): The appraised value of the property.
            tax_amount (float | int | str): The amount of delinquent taxes.

        Raises:
            ValueError: If inputs cannot be converted to numbers or are invalid
                        (e.g., negative appraised value, negative tax amount).
        """
        # Clean and convert the input values
        self._appraised_value = clean_numeric_value(appraised_value)
        self._tax_amount = clean_numeric_value(tax_amount)
        
        # If either value is "Unknown", set both to "Unknown" and return
        if self._appraised_value == "Unknown" or self._tax_amount == "Unknown":
            self._appraised_value = "Unknown"
            self._tax_amount = "Unknown"
            self._delinquency_ratio = "Unknown"
            return
            
        # Basic validation
        if self._appraised_value < 0:
            # While technically possible in extreme market crashes,
            # a negative appraisal usually indicates bad data for this ratio.
            raise ValueError("Appraised value cannot be negative for ratio calculation.")
        if self._tax_amount < 0:
            raise ValueError("Delinquent tax amount cannot be negative.")

        # Calculate the core custom metric: delinquency ratio
        self._delinquency_ratio = self._calculate_ratio()

    def _calculate_ratio(self):
        """
        Calculates the delinquency ratio (tax_amount / appraised_value).
        Handles division by zero if appraised value is 0.
        """
        if self._appraised_value == 0:
            # If value is zero and taxes are owed, the ratio is effectively infinite.
            # If value is zero and taxes are zero, ratio is undefined (or zero).
            # Returning None is a safe way to indicate it's not calculable meaningfully here.
            return None
        elif self._tax_amount == 0:
             # If no tax is due, the ratio is clearly 0
             return 0.0
        else:
            # Calculate the ratio
            return self._tax_amount / self._appraised_value

    # --- Properties to access data ---
    @property
    def appraised_value(self):
        """Returns the appraised value."""
        return self._appraised_value

    @property
    def tax_amount(self):
        """Returns the delinquent tax amount."""
        return self._tax_amount

    @property
    def delinquency_ratio(self):
        """
        Returns the ratio of delinquent tax amount to appraised value.
        Returns None if appraised value is zero.
        """
        return self._delinquency_ratio

    @property
    def delinquency_percentage(self):
        """
        Returns the delinquency ratio as a percentage.
        Returns None if the ratio could not be calculated.
        """
        if isinstance(self._delinquency_ratio, str) and self._delinquency_ratio == "Unknown":
            return "Unknown"
            
        if self._delinquency_ratio is None:
            return None
        return self._delinquency_ratio * 100

    # --- Optional: Add methods for interpretation ---
    def get_risk_category(self, thresholds=None):
         """
         Categorizes the delinquency risk based on the ratio percentage.

         Args:
             thresholds (dict, optional): A dictionary defining the upper bounds
                 (as percentages) for risk categories.
                 Example: {'Low': 2.0, 'Medium': 5.0, 'High': 10.0}.
                 Defaults are provided if None. Anything above the highest
                 threshold is considered 'Very High'.

         Returns:
             str: The risk category ('N/A', 'None', 'Low', 'Medium', 'High', 'Very High').
         """
         if isinstance(self._delinquency_ratio, str) and self._delinquency_ratio == "Unknown":
             return "Unknown"
             
         percentage = self.delinquency_percentage

         if percentage is None:
             return "N/A (Invalid Value)"
         if percentage == 0:
             return "None"

         if thresholds is None:
             # Default thresholds (e.g., 2%, 5%, 10%) - Adjust as needed!
             thresholds = {'Low': 2.0, 'Medium': 5.0, 'High': 10.0}

         # Ensure thresholds are sorted by value for correct categorization
         sorted_thresholds = sorted(thresholds.items(), key=lambda item: item[1])

         for category, upper_bound_percent in sorted_thresholds:
             if percentage <= upper_bound_percent:
                 return category

         # If percentage is higher than all defined thresholds
         return "Very High"

    # --- String representations ---
    def __str__(self):
        """Returns a user-friendly string representation."""
        if isinstance(self._appraised_value, str) and self._appraised_value == "Unknown":
            return "Appraised Value: Unknown\nDelinquent Tax: Unknown\nDelinquency Ratio: Unknown\nRisk Category: Unknown"
            
        val_str = locale.currency(self.appraised_value, grouping=True)
        tax_str = locale.currency(self.tax_amount, grouping=True)

        if self.delinquency_percentage is not None:
            ratio_str = f"{self.delinquency_percentage:.2f}%"
            risk_str = self.get_risk_category() # Use default thresholds here
        else:
            ratio_str = "N/A"
            risk_str = "N/A (Invalid Value)"

        return (f"Appraised Value: {val_str}\n"
                f"Delinquent Tax: {tax_str}\n"
                f"Delinquency Ratio: {ratio_str}\n"
                f"Risk Category: {risk_str}")

    def __repr__(self):
        """Returns a developer-friendly string representation."""
        return (f"{self.__class__.__name__}("
                f"appraised_value={self.appraised_value!r}, "
                f"tax_amount={self.tax_amount!r})")

# Create a convenience method for processing May_2025_Amount
def calculate_delinquency_for_may_2025(appraised_value, may_2025_amount):
    """
    Convenience method for calculating delinquency metrics using May_2025_Amount.
    
    Args:
        appraised_value (float | int | str): The appraised value of the property
        may_2025_amount (float | int | str): The May 2025 tax amount
        
    Returns:
        DelinquencyDataPoint: The calculated metrics
    """
    return DelinquencyDataPoint(appraised_value, may_2025_amount)

# --- Example Usage ---
if __name__ == "__main__":
    try:
        # Example 1: A typical case
        home1 = DelinquencyDataPoint(appraised_value=450000, tax_amount=9000)
        print("--- Home 1 ---")
        print(home1)
        print(f"Raw Ratio: {home1.delinquency_ratio:.4f}")
        print(f"Repr: {repr(home1)}")
        print("-" * 20)

        # Example 2: Higher risk
        home2 = DelinquencyDataPoint(appraised_value=200000, tax_amount=15000)
        print("--- Home 2 ---")
        print(home2)
        print("-" * 20)

        # Example 3: No delinquency
        home3 = DelinquencyDataPoint(appraised_value=600000, tax_amount=0)
        print("--- Home 3 ---")
        print(home3)
        print("-" * 20)

        # Example 4: Edge case - Zero appraised value (e.g., data error or worthless land)
        home4 = DelinquencyDataPoint(appraised_value=0, tax_amount=1000)
        print("--- Home 4 (Zero Value) ---")
        print(home4)
        print(f"Raw Ratio: {home4.delinquency_ratio}") # Should be None
        print(f"Percentage: {home4.delinquency_percentage}") # Should be None
        print("-" * 20)

        # Example 5: Using custom risk thresholds
        custom_thresholds = {'Minor': 1.0, 'Concerning': 3.0, 'Serious': 7.5}
        home5 = DelinquencyDataPoint(appraised_value=300000, tax_amount=10000) # 3.33% ratio
        print("--- Home 5 (Custom Risk) ---")
        print(home5) # Uses default thresholds in __str__
        print(f"Custom Risk Category: {home5.get_risk_category(thresholds=custom_thresholds)}") # Should be 'Serious'
        print("-" * 20)
        
        # Example 6: Using May_2025_Amount 
        home6_may_2025 = DelinquencyDataPoint(appraised_value=350000, tax_amount=12000)
        print("--- Home 6 (Using May 2025 Amount) ---")
        print(home6_may_2025)
        print("-" * 20)
        
        # Example 7: Unknown values
        home7 = DelinquencyDataPoint(appraised_value="Unknown", tax_amount=8000)
        print("--- Home 7 (Unknown Value) ---")
        print(home7)
        print(f"Ratio: {home7.delinquency_ratio}")
        print(f"Percentage: {home7.delinquency_percentage}")
        print(f"Risk: {home7.get_risk_category()}")
        print("-" * 20)

    except ValueError as e:
        print(f"Error creating data point: {e}")

    # Example 8: Invalid input handling
    try:
        print("--- Invalid Input Example ---")
        invalid_home = DelinquencyDataPoint(appraised_value="Expensive", tax_amount=5000)
        print(invalid_home)
    except ValueError as e:
        print(f"Caught expected error: {e}")
    print("-" * 20)