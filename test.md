# Recommended Folder Structure with Roles and Guidelines

## Root Level

```
app/
components/
hooks/
lib/
store/
utils/
styles/
public/
```

---

## Full Project Folder Structure

```
app/
  layout.tsx
  page.tsx
  dashboard/
    layout.tsx
    page.tsx
  [id]/
    page.tsx
    loading.tsx
    error.tsx

components/
  common/
    Button.tsx
    Modal.tsx
  users/
    UserCard.tsx
    UserList.tsx
  dashboard/
    AnalyticsChart.tsx

hooks/
  useAuth.ts
  useFetch.ts
  useDebounce.ts
  useToggle.ts

lib/
  constants/
    routes.ts
  helpers/
    dateHelper.ts
    stringHelper.ts

store/
  user/
    userStore.ts
  auth/
    authStore.ts

utils/
  validation/
    formValidation.ts
  formatting/
    dateFormatter.ts

styles/
  globals.css
  components/
    Button.module.css
  pages/
    DashboardPage.module.css

public/
  images/
    logos/
      logo.svg
    avatars/
      default-avatar.png
  fonts/
    OpenSans-Regular.ttf
```

---

## 1. `app/` Folder

### Role: Routing and Page Logic

- Facilitates **route definitions**, layouts, and server-side rendering.
- Pages must:
  - Utilize reusable components from the `components` folder.
  - Leverage utility functions from `utils` where applicable.
  - Employ hooks from the `hooks` folder for logic management.

### Rules:

1. **Layout and Structure:**
   - Use `layout.tsx` to define shared layouts for child routes.
   - Refrain from embedding styles in `page.tsx`; prefer Tailwind CSS or `styles/`.
2. **Dynamic Routes:**
   - Implement dynamic routes for resources requiring unique identifiers (e.g., `[id]`).
3. **Loading and Error States:**
   - Utilize `loading.tsx` and `error.tsx` to manage asynchronous fetching and error handling scenarios.

---

## 2. `components/` Folder

### Role: Reusable UI Components

- Dedicated to **presentation logic**.
- Must exclude business logic; data fetching and state processing occur in parent components.
- Organized into:
  - `common/`: General-purpose UI components like `Button` and `Modal`.
  - Feature-specific subdirectories (e.g., `users/UserCard`).

### Rules:

1. **Prioritize Reusability:**
   - Ensure components are reusable before creating new ones.
2. **Promote Composition:**
   - Develop smaller, composable components instead of large, monolithic structures.
3. **Tailwind Integration:**
   - Consistently apply Tailwind CSS classes across all components.

---

## 3. `hooks/` Folder

### Role: Custom Hooks

- Houses **reusable logic** for state and side effects.
- Hooks must adhere to the **React naming convention** (`useSomething`).
- Utilized for:
  - **Data fetching** (e.g., `useFetch`, `useQuery`).
  - **State management** (e.g., `useAuth`, `useToggle`).

### Rules:

1. **Encapsulate Logic:**
   - Condense repetitive patterns, such as form handling or state derivation, into hooks.
2. **Avoid Redundancy:**
   - Hooks should not supplant straightforward utility functions.

---

## 4. `lib/` Folder

### Role: Business Logic and Utilities

- Centralizes **business logic** and **helper functions**.
- Handles complex reusable logic across the application.
- Subfolders:
  - `constants/`: Route definitions, enums, and static values.
  - `helpers/`: General utility functions.

### Rules:

1. **Exclude UI Logic:**
   - `lib` is strictly for logic unrelated to UI rendering.
2. **Domain Organization:**
   - Group files logically by functionality (e.g., `constants`, `helpers`).

---

## 5. `store/` Folder

### Role: Global State Management

- Contains **global state logic**, leveraging libraries like Zustand or Redux.
- Segregates slices or modules by domain (e.g., `userStore`, `authStore`).

### Rules:

1. **Enforce Modularity:**
   - Prevent bundling all state into a single file.
2. **Limit UI State:**
   - Reserve local UI state (e.g., modals) for components rather than global stores.

---

## 6. `utils/` Folder

### Role: Helper Functions

- Provides **pure utility functions** for application-wide use.
- Structured into subdirectories by functionality (e.g., `validation`, `formatting`).

### Rules:

1. **Avoid Side Effects:**
   - Utility functions must not rely on global state or produce side effects.
2. **Optimize Reusability:**
   - Ensure functions are concise and universally applicable.

---

## 7. `styles/` Folder

### Role: Supplemental Styling (if Tailwind is insufficient)

- Stores global CSS, themes, and scoped styles as needed.
- Primarily relies on Tailwind CSS for styling.

### Tailwind CSS Configuration Guidelines:

1. **Centralized Customization:**
   - Consolidate custom colors, animations, and spacing in `tailwind.config.js`.
2. **Custom Colors:**
   - Extend `theme.extend.colors` for project-specific color palettes.
3. **Reusable Animations:**
   - Define animations in `theme.extend.animation`.
4. **Spacing Adjustments:**
   - Add custom spacing values to `theme.spacing`.

### Example Tailwind Configuration:

```javascript
// tailwind.config.js
module.exports = {
  content: ["./app/**/*.{js,ts,jsx,tsx}", "./components/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        primary: "#1D4ED8",
        secondary: "#9333EA",
      },
      animation: {
        fadeIn: "fadeIn 0.5s ease-in",
      },
      keyframes: {
        fadeIn: {
          from: { opacity: 0 },
          to: { opacity: 1 },
        },
      },
    },
  },
  plugins: [],
};
```

---

## 8. `public/` Folder

### Role: Static Assets

- Manages static assets such as images, fonts, and external files.
- Organizes content by type.

### Rules:

1. **Asset-Only:**
   - Avoid adding JS or TS files; restrict usage to assets.
2. **Categorized Storage:**
   - Classify images, fonts, and static files into subdirectories.

---

## General Coding Guidelines

1. **Decompose Complexity:**

   - Break down pages and components into smaller, composable entities.
   - Example: Use hooks, components, and utilities instead of embedding all logic in `page.tsx`.

2. **Adopt Consistent Naming:**

   - Apply `camelCase` for variables and functions.
   - Employ `PascalCase` for components.

3. **Define Clear Roles:**

   - Pages (`app/`) orchestrate logic but avoid implementing it.
   - Components manage UI but not application logic.

4. **Prioritize Reusability:**

   - Confirm existing implementations before adding new logic.

5. **Centralize Types and Interfaces:**

   - Maintain all type definitions and interfaces within a `types/` directory.
   - Divide types into domain-specific subfolders (e.g., `models/`, `responses/`).

---
