#!/bin/bash
# Function to submit a commit status to GitHub
submit_commit_status() {

  print_help() {
    echo -e "Usage: $0 [-h] [-r REPO_NAME] [-o ORG_NAME] [-u GIT_USER] [-t GIT_TOKEN] [-s COMMIT_STATUS] [-c CONTEXT] [-d DESCRIPTION]"
    echo -e "  -h\tPrint this help message"
    echo -e "  -r\tThe repository name"
    echo -e "  -o\tThe organisation name"
    echo -e "  -u\tThe GitHub username"
    echo -e "  -t\tThe GitHub token"
    echo -e "  -s\tThe commit status"
    echo -e "  -c\tThe context"
    echo -e "  -d\t[OPTIONAL] The description"
  }

  if [[ "$1" == "-h" ]]
  then
    print_help
    return 0
  fi

  while [ $# -gt 0 ]
  do
    case $1 in
      -h)
        print_help
        exit 0
        ;;
      -r)
        shift
        repo_name=$1
        ;;
      -o)
        shift
        org_name=$1
        ;;
      -u)
        shift
        git_user=$1
        ;;
      -t)
        shift
        git_token=$1
        ;;
      -s)
        shift
        commit_status=$1
        ;;
      -c)
        shift
        context=$1
        ;;
      -d)
        shift
        description=$1
        ;;
      *)
        echo "ERROR: Invalid argument $1"
        exit 1
        ;;
    esac
    shift
  done

  if [ -z "$repo_name" ] || [ -z "$org_name" ] || [ -z "$git_user" ] || [ -z "$git_token" ] || [ -z "$commit_status" ] || [ -z "$context" ]; then
    echo "ERROR: Missing mandatory parameter(s)"
    echo ""
    print_help
    return 1
  fi

  curl -sS -u "$git_user:$git_token" \
    -X POST \
    -H "Accept: application/vnd.github.v3+json" \
    https://api.github.com/repos/$org_name/$repo_name/statuses/$(git rev-parse HEAD) \
    -d '{
      "state": "'"$commit_status"'",
      "context": "'"$context"'",
      "description": "'"$description"'"
    }' &> /dev/null
}

# Function to submit an issue to GitHub
submit_issue() {

  print_help() {
    echo -e "Usage: $0 [-h] [-r REPO_NAME] [-o ORG_NAME] [-u GIT_USER] [-t GIT_TOKEN] [-s COMMIT_STATUS] [-c CONTEXT]"
    echo -e "  -h\tPrint this help message"
    echo -e "  -r\tThe repository name"
    echo -e "  -o\tThe organisation name"
    echo -e "  -u\tThe GitHub username"
    echo -e "  -t\tThe GitHub token"
    echo -e "  -i\tThe issue title"
    echo -e "  -c\tThe issue content in markdown"
    echo -e "  -l\t[OPTIONAL] The issue labels"
  }

  if [[ "$1" == "-h" ]]
  then
    print_help
    return 0
  fi

  local repo_name
  local org_name
  local git_user
  local git_token
  local title
  local markdown_content
  local labels

  while [ $# -gt 0 ]
  do
    case $1 in
      -h)
        print_help
        exit 0
        ;;
      -r)
        shift
        repo_name=$1
        ;;
      -o)
        shift
        org_name=$1
        ;;
      -u)
        shift
        git_user=$1
        ;;
      -t)
        shift
        git_token=$1
        ;;
      -i)
        shift
        title=$1
        ;;
      -c)
        shift
        markdown_content=$1
        ;;
      -l)
        shift
        if [[ -z $labels ]]
        then
          echo "Setting labels to $1"
          labels=($1)
        else
          echo "Appending label $1"
          labels+=($1)
        fi
        ;;
      *)
        echo "ERROR: Invalid argument $1"
        exit 1
        ;;
    esac
    shift
  done

  if [ -z "$repo_name" ] || [ -z "$org_name" ] || [ -z "$git_user" ] || [ -z "$git_token" ] || [ -z "$title" ] || [ -z "$markdown_content" ]; then
    echo "ERROR: Missing mandatory parameter(s)"
    return 1
  fi

  if [[ ${#labels[@]} -gt 0 ]]
  then
    labels_json=$(printf '%s\n' "${labels}" | jq -R . | jq -s .)
  else
    labels_json="[]"
  fi

  curl -sS -u "$git_user:$git_token" \
    -X POST \
    -H "Accept: application/vnd.github.v3+json" \
    https://api.github.com/repos/$org_name/$repo_name/issues \
    -d '{
      "title": "'"$title"'",
      "body": "'"$markdown_content"'",
      "labels": '"$labels_json"'
    }' &> /dev/null
}