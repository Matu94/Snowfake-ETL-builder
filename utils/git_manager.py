import streamlit as st
from github import Github
import base64

def push_to_github(file_path, file_content, commit_message):
    try:
        #Connect
        token = st.secrets["github"]["token"]
        repo_name = st.secrets["github"]["repo_name"]
        branch = st.secrets["github"]["branch"]
        
        g = Github(token)
        repo = g.get_repo(repo_name)

        #Check if file exists to decide: Create or Update?
        try:
            contents = repo.get_contents(file_path, ref=branch)
            # UPDATE existing file
            repo.update_file(
                path=file_path,
                message=commit_message,
                content=file_content,
                sha=contents.sha,
                branch=branch
            )
            return f"Success! Updated {file_path} on GitHub!"
            
        except:
            #CREATE new file
            repo.create_file(
                path=file_path,
                message=commit_message,
                content=file_content,
                branch=branch
            )
            return f"Success! Created {file_path} on GitHub!"

    except Exception as e:
        return f"Git Error: {str(e)}"