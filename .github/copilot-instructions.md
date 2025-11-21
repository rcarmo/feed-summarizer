This project is an RSS feed summarizer that fetches, processes, and publishes news summaries using AI.

There is a `SPEC.md` file that describes the system architecture, components, and workflow in detail. The key points to note are:

* The project is designed to run as a background service.
* The outputs include both HTML bulletins and RSS feeds.
* It uses Azure OpenAI for AI-powered summarization, with a small set of built-in libraries.
* You should not add additional dependencies beyond those listed in the `requirements.txt` file.
* You should not implement additional functionality beyond what is asked for unless it is part of a larger feature or improvement. When in doubt, give the user a list of possible choices.

## Coding Style Guidelines

* You must use explicit imports; do not use wildcard imports or import entire modules without detailing what you need.
* Never use inline imports. All imports should be at the top of the file.
* You must not resort to inline imports except in very specific cases (e.g., to avoid circular dependencies).
* You should default to asyncio code where possible.
* You should prefer a functional coding style (functions in clearly scoped modules) to over-use of classes

## RSS and Feed Reader Best Practices

The implementation follows best practices as described in:

- [HTTP conditional request best practices](http://rachelbythebay.com/w/2023/01/18/http/)
- [Feed reader behavior best practices](http://rachelbythebay.com/w/2024/05/27/feed/)
- [Feed reader scoring criteria](http://rachelbythebay.com/w/2024/05/30/fs/)

