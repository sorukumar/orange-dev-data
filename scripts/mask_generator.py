from PIL import Image, ImageDraw
import os

def crop_to_circle(image_path, output_path):
    # Open the image
    img = Image.open(image_path).convert("RGBA")
    
    # Create matching size black image with white circle for mask
    mask = Image.new('L', img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0) + img.size, fill=255)
    
    # Apply mask
    img.putalpha(mask)
    
    # Save as PNG
    img.save(output_path, "PNG")

assets_dir = os.path.join(os.path.dirname(__file__), "../assets")
files = ["satoshi.png", "gavin_andresen.png", "wladimir.png", "marcofalke.png", "michael_ford.png", "pieter_wuille.png"]

for f in files:
    input_p = os.path.join(assets_dir, f)
    if os.path.exists(input_p):
        print(f"Cropping {f}...")
        # Overwrite with circular PNG
        crop_to_circle(input_p, input_p)
