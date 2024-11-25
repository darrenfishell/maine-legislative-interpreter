# Maine Bills Repository

This repository contains text extracted from the PDFs of bills hosted by 

## Using This Data

I recommend pulling this data into your project one of two ways.

### Option 1: Clone

Clone the full repository to a location of your choice:
```
git clone https://github.com/PhilipMathieu/maine-bills.git
```

### Option 2: Use a Git Submodule

This method will maintain the connection between the dataset and this original repository, allowing you to pull updates as they are made. To clone the repository into a subdirectory called "data", for example, run the following:

```
cd [your project directory]
mkdir data
git submodule add [https://github.com/PhilipMathieu/maine-bills](https://github.com/PhilipMathieu/maine-bills) data
```

For more on Git Submodules, see [this GitHub blog post](https://github.blog/2016-02-01-working-with-submodules/).

## How It Works

### src/scraper.py

This script does the bulk of the work downloading the PDFs, extracting the text, and saving the output. I suggest reading through the script to understand more about how this works.

### .github/workflows/run-with-conda.yml
This is a [GitHub Actions](https://docs.github.com/en/actions) workflow that sets up a Linux environment, installs Conda, and runs the scraper. I suggest reading the yml file to understand more about how this works.

## Contributing

If you think of a useful improvement to this project, please feel free to fork and pull request or create an issue.

## License
The data in this repository is extracted from the PDFs provided by the State of Maine's [Law and Legislative Reference Library](https://legislature.maine.gov/lawLibrary). To the best of my knowledge, this project does not violate any terms of use or copyright law. However, please be aware that this project is neither supported nor explicitely authorized by the Library or by the Maine State Legislature.

All other content, including code, is licensed under the MIT License:

Copyright 2023 Philip Mathieu

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
